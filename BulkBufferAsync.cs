using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

public sealed class BulkBufferAsync : IDisposable
{
    private readonly string _connStr, _owner, _table;
    private readonly int _batchSize;
    private readonly object _sync = new object();
    private readonly DataTable _schema;
    private DataTable _buf;
    private int _count;
    private bool _disposed;

    private readonly string[] _keyColumns;
    private readonly HashSet<string> _seen = new HashSet<string>(StringComparer.Ordinal);
    private readonly bool _clearSeenOnFlush = false;

    private readonly SemaphoreSlim _flushGate = new SemaphoreSlim(1, 1);
    private readonly List<Task> _inflight = new List<Task>();

    public int BulkCopyTimeoutSeconds { get; set; } = 3600;

    public BulkBufferAsync(string connStr, string owner, string table, int batchSize = 5000)
    {
        _connStr = connStr; _owner = owner; _table = table; _batchSize = batchSize;

        using var conn = new OracleConnection(_connStr);
        conn.Open();
        _schema = BuildSchema(conn, _owner, _table);
        _buf = _schema.Clone();
        _keyColumns = LoadPkColumns(conn, _owner, _table);
    }

    public void Add(IDictionary<string, object?> values)
    {
        lock (_sync)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(BulkBufferAsync));

            var key = BuildKey(values);
            if (key != null && !_seen.Add(key)) return;

            var row = _buf.NewRow();
            foreach (var kv in values)
                if (_buf.Columns.Contains(kv.Key))
                    row[kv.Key] = kv.Value ?? DBNull.Value;

            _buf.Rows.Add(row);
            _count++;

            if (_count >= _batchSize)
                Flush_NoLock_Async();
        }
    }

    public Task FlushAsync()
    {
        lock (_sync)
        {
            if (_disposed) return Task.CompletedTask;
            return Flush_NoLock_Async();
        }
    }

    public void Flush()
    {
        Task t;
        lock (_sync)
        {
            if (_disposed) return;
            t = Flush_NoLock_Async();
        }
        t?.Wait();
    }

    private Task Flush_NoLock_Async()
    {
        if (_count == 0) return Task.CompletedTask;

        var toFlush = _buf;
        _buf = _schema.Clone();
        _count = 0;
        if (_clearSeenOnFlush) _seen.Clear();

        var task = Task.Run(async () =>
        {
            await _flushGate.WaitAsync().ConfigureAwait(false);
            try
            {
                using var conn = new OracleConnection(_connStr);
                conn.Open();
                using var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
                {
                    DestinationTableName = $"{_owner}.{_table}",
                    BatchSize = Math.Max(1, toFlush.Rows.Count),
                    BulkCopyTimeout = Math.Max(1, BulkCopyTimeoutSeconds)
                };
                bulk.WriteToServer(toFlush);
            }
            finally
            {
                toFlush.Dispose();
                _flushGate.Release();
            }
        });

        _inflight.Add(task);
        task.ContinueWith(_ =>
        {
            lock (_inflight) _inflight.Remove(task);
        }, TaskScheduler.Default);

        return task;
    }

    public void Dispose()
    {
        lock (_sync) _disposed = true;

        Task last = Flush_NoLock_Async();

        List<Task> pending;
        lock (_inflight) pending = _inflight.ToList();
        if (last != null) pending.Add(last);
        if (pending.Count > 0) Task.WaitAll(pending.ToArray());

        _buf?.Dispose();
        _flushGate.Dispose();
    }

    private static DataTable BuildSchema(OracleConnection conn, string owner, string table)
    {
        const string sql = @"
            SELECT column_name, data_type, data_precision, data_scale,
                   identity_column, virtual_column
            FROM   all_tab_columns
            WHERE  owner = :own AND table_name = :t
            ORDER  BY column_id";
        using var cmd = new OracleCommand(sql, conn) { BindByName = true };
        cmd.Parameters.Add("own", owner.ToUpperInvariant());
        cmd.Parameters.Add("t", table.ToUpperInvariant());

        using var r = cmd.ExecuteReader();
        var dt = new DataTable(table);
        while (r.Read())
        {
            var name = r.GetString(0);
            var typ  = r.GetString(1);
            int? p   = r.IsDBNull(2) ? null : (int?)Convert.ToInt32(r.GetDecimal(2));
            int? s   = r.IsDBNull(3) ? null : (int?)Convert.ToInt32(r.GetDecimal(3));
            var idn  = r.GetString(4) == "YES";
            var virt = r.GetString(5) == "YES";
            if (idn || virt) continue;
            dt.Columns.Add(name, MapOracleToDotNet(typ, p, s));
        }
        return dt;
    }

    private static string[] LoadPkColumns(OracleConnection conn, string owner, string table)
    {
        const string sql = @"
            SELECT cc.column_name
            FROM   all_constraints c
            JOIN   all_cons_columns cc
              ON   cc.owner = c.owner
             AND   cc.constraint_name = c.constraint_name
             AND   cc.table_name = c.table_name
            WHERE  c.owner = :own
              AND  c.table_name = :tbl
              AND  c.constraint_type = 'P'
            ORDER  BY cc.position";
        using var cmd = new OracleCommand(sql, conn) { BindByName = true };
        cmd.Parameters.Add("own", owner.ToUpperInvariant());
        cmd.Parameters.Add("tbl", table.ToUpperInvariant());
        using var r = cmd.ExecuteReader();
        var list = new List<string>();
        while (r.Read()) list.Add(r.GetString(0));
        return list.ToArray();
    }

    private static Type MapOracleToDotNet(string oracleType, int? precision, int? scale) => oracleType switch
    {
        "NUMBER" => (scale ?? 0) > 0 ? typeof(decimal)
                   : (precision ?? 0) == 0 ? typeof(decimal)
                   : precision <= 9 ? typeof(int)
                   : precision <= 18 ? typeof(long)
                   : typeof(decimal),
        "FLOAT" => typeof(double),
        "DATE" or "TIMESTAMP" or "TIMESTAMP(6)" => typeof(DateTime),
        "VARCHAR2" or "NVARCHAR2" or "CHAR" or "NCHAR" or "CLOB" or "NCLOB" => typeof(string),
        "BLOB" or "RAW" => typeof(byte[]),
        _ => typeof(string)
    };

    private string? BuildKey(IDictionary<string, object?> values)
    {
        if (_keyColumns == null || _keyColumns.Length == 0) return null;
        var parts = new string[_keyColumns.Length];
        for (int i = 0; i < _keyColumns.Length; i++)
        {
            var col = _keyColumns[i];
            if (values == null || !values.TryGetValue(col, out var v) || v == null || v is DBNull)
                parts[i] = "<NULL>";
            else if (v is string s)
                parts[i] = s.Trim().ToUpperInvariant();
            else
                parts[i] = Convert.ToString(v, CultureInfo.InvariantCulture) ?? "";
        }
        return string.Join("|", parts);
    }
}
