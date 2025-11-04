using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

public sealed class AsyncOracleFlusher : IAsyncDisposable
{
    private readonly SemaphoreSlim _flushGate = new(4);              // 동시 flush 최대 4 (원하는 병렬도로 조정)
    private readonly ConcurrentDictionary<int, Task> _inflight = new();
    private readonly object _swapLock = new();
    private int _flushAsyncCount;
    private volatile int _count;                                     // 현재 _buf의 레코드 수
    private DataTable _buf;                                          // 현재 적재 중 버퍼
    private readonly DataTable _schema;                              // _buf 스키마(초기 clone 원본)
    private readonly bool _clearSeenOnFlush;
    private readonly string _connStr;
    private readonly string _tableName = "YOUR_TABLE";
    private readonly int _bulkBatchSize = 5000;                      // 필요 시 조정

    public AsyncOracleFlusher(DataTable schema, string connStr, bool clearSeenOnFlush = false)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _buf = _schema.Clone();
        _connStr = connStr ?? throw new ArgumentNullException(nameof(connStr));
        _clearSeenOnFlush = clearSeenOnFlush;
    }

    public Task FlushAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref _count) == 0)
            return Task.CompletedTask;

        DataTable toFlush;
        lock (_swapLock)
        {
            if (_count == 0)
                return Task.CompletedTask;

            toFlush = _buf;
            _buf = _schema.Clone();
            _count = 0;
        }

        if (_clearSeenOnFlush)
            _seen?.Clear();

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
                BulkCopyTimeout = 5000
            };

            await Task.Run(() => bulk.WriteToServer(toFlush), ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            try
            {
                toFlush.Clear();
                toFlush.Dispose();
            }
            catch { }

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
                    catch { }
                }
            }

            _flushGate.Release();
        }
    }

    public async Task DrainAsync()
    {
        var list = _inflight.Values.ToArray();
        if (list.Length > 0)
            await Task.WhenAll(list).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await DrainAsync().ConfigureAwait(false);
        _flushGate.Dispose();
    }
}
