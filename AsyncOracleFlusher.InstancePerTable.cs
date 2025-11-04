using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace BulkFlush
{
    /// <summary>
    /// Per-table instance flusher.
    /// - One instance == one destination table + its own in-memory buffer(DataTable).
    /// - Bounded concurrency with SemaphoreSlim.
    /// - Safe buffer swap on FlushAsync.
    /// - Supports both IDisposable and IAsyncDisposable.
    /// </summary>
    public sealed class AsyncOracleFlusher : IDisposable, IAsyncDisposable
    {
        private readonly SemaphoreSlim _flushGate;
        private readonly ConcurrentDictionary<int, Task> _inflight = new();
        private readonly object _swapLock = new();

        private int _flushAsyncCount;
        private volatile int _count;             // number of rows in _buf

        private DataTable _buf;                  // current accumulating buffer
        private readonly DataTable _schema;      // schema template to clone new buffers
        private bool _disposed;

        private readonly string _connStr;
        private readonly string _tableName;
        private readonly int _bulkBatchSize;
        private readonly int _bulkTimeoutSeconds;

        public AsyncOracleFlusher(
            DataTable schema,
            string tableName,
            string connStr,
            int degreeOfParallelism = 4,
            int bulkBatchSize = 5000,
            int bulkTimeoutSeconds = 5000)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _buf = _schema.Clone();
            _tableName = !string.IsNullOrWhiteSpace(tableName) ? tableName : throw new ArgumentNullException(nameof(tableName));
            _connStr = !string.IsNullOrWhiteSpace(connStr) ? connStr : throw new ArgumentNullException(nameof(connStr));
            _bulkBatchSize = bulkBatchSize;
            _bulkTimeoutSeconds = bulkTimeoutSeconds;
            _flushGate = new SemaphoreSlim(Math.Max(1, degreeOfParallelism));
        }

        /// <summary>Add a single DataRow into the in-memory buffer (thread-safe).</summary>
        public void AddRow(DataRow row)
        {
            if (row is null) return;
            lock (_swapLock)
            {
                _buf.ImportRow(row);
                Interlocked.Increment(ref _count);
            }
        }

        /// <summary>Add many rows from another DataTable (thread-safe).</summary>
        public void AddRows(DataTable table)
        {
            if (table is null || table.Rows.Count == 0) return;
            lock (_swapLock)
            {
                foreach (DataRow r in table.Rows)
                {
                    _buf.ImportRow(r);
                }
                Interlocked.Add(ref _count, table.Rows.Count);
            }
        }

        /// <summary>
        /// Flush the current buffer to Oracle asynchronously.
        /// Returns a task representing the flush of the current snapshot.
        /// </summary>
        public Task FlushAsync(CancellationToken ct = default)
        {
            if (Volatile.Read(ref _count) == 0)
                return Task.CompletedTask;

            DataTable toFlush;
            lock (_swapLock)
            {
                if (_count == 0)
                    return Task.CompletedTask;

                toFlush = _buf;                 // snapshot
                _buf = _schema.Clone();         // new buffer
                _count = 0;
            }

            var t = FlushCoreAsync(toFlush, ct);
            _inflight[t.Id] = t;
            _ = t.ContinueWith(_ => _inflight.TryRemove(t.Id, out _), TaskScheduler.Default);
            return t;
        }

        private async Task FlushCoreAsync(DataTable toFlush, CancellationToken ct)
        {
            await _flushGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                using var conn = new OracleConnection(_connStr);
                try { await conn.OpenAsync(ct).ConfigureAwait(false); }
                catch (NotSupportedException) { conn.Open(); }

                using var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
                {
                    DestinationTableName = _tableName,
                    BatchSize = _bulkBatchSize,
                    BulkCopyTimeout = _bulkTimeoutSeconds
                };

                // OracleBulkCopy.WriteToServer is typically synchronous.
                await Task.Run(() => bulk.WriteToServer(toFlush), ct).ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    toFlush.Clear();
                    toFlush.Dispose();
                }
                catch { /* best-effort */ }

                var n = Interlocked.Increment(ref _flushAsyncCount);
                if ((n % 10) == 0)
                {
                    lock (_swapLock)
                    {
                        try
                        {
                            var fresh = _schema.Clone();
                            _buf?.Dispose();
                            _buf = fresh;
                        }
                        catch { /* best-effort */ }
                    }
                }

                _flushGate.Release();
            }
        }

        /// <summary>Await all in-flight flushes (call on shutdown).</summary>
        public async Task DrainAsync()
        {
            var list = _inflight.Values.ToArray();
            if (list.Length > 0)
                await Task.WhenAll(list).ConfigureAwait(false);
        }

        // ===== Dispose Pattern =====

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            try
            {
                // Ensure all inflight operations complete.
                DrainAsync().GetAwaiter().GetResult();
            }
            catch { /* swallow on dispose */ }
            finally
            {
                _buf?.Dispose();
                _flushGate.Dispose();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;
            try
            {
                await DrainAsync().ConfigureAwait(false);
            }
            finally
            {
                _buf?.Dispose();
                _flushGate.Dispose();
            }
        }
    }
}
