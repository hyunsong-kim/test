// BulkBuffer with snapshot-swap Flush (버퍼 스왑 방식)
// - Add(): lock 짧게 잡고 _buf에 행 추가
// - Flush(): 현재 _buf를 떼어내 스냅샷으로 DB에 쓰고, 새 DataTable로 교체
// - Flush 중에도 Add()는 새 버퍼에 계속 쌓임
//
// NuGet: Oracle.ManagedDataAccess
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using Oracle.ManagedDataAccess.Client;

public sealed class BulkBuffer : IDisposable
{
    private readonly string _connStr;
    private readonly string _owner;
    private readonly string _table;
    private readonly int _batchSize;
    private readonly object _sync = new object();
    private readonly DataTable _schema;
    private DataTable _buf;
    private int _count;
    private readonly HashSet<string> _seen = new(StringComparer.Ordinal);
    private readonly string[] _keyColumns;
    private readonly bool _clearSeenOnFlush = false; // false=전역 멱등 유지

    public BulkBuffer(string connStr, string owner, string table, int batchSize = 5000)
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
            var key = BuildKey(values);
            if (key != null && !_seen.Add(key)) return;

            var row = _buf.NewRow();
            foreach (var kv in values)
                if (_buf.Columns.Contains(kv.Key))
                    row[kv.Key] = kv.Value ?? DBNull.Value;
            _buf.Rows.Add(row);
            _count++;

            if (_count >= _batchSize)
                Flush_NoLock();
        }
    }

    public void Flush()
    {
        lock (_sync) Flush_NoLock();
    }

    private void Flush_NoLock()
    {
        if (_count == 0) return;
        var toFlush = _buf;            // 스냅샷
        _buf = _schema.Clone();        // 새 버퍼
        _count = 0;
        if (_clearSeenOnFlush) _seen.Clear();

        // DB I/O는 lock 밖에서
        using var conn = new OracleConnection(_connStr);
        conn.Open();
        using var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
        {
            DestinationTableName = $"{_owner}.{_table}",
            BatchSize = Math.Max(1, toFlush.Rows.Count),
            BulkCopyTimeout = 3600 // 1시간 (0=기본30초)
        };
        bulk.WriteToServer(toFlush);
        toFlush.Dispose();
    }

    private static DataTable BuildSchema(OracleConnection conn, string owner, string table)
    {
        const string sql = @"
            SELECT column_name, data_type, data_precision, data_scale,
                   identity_column, virtual_column
              FROM all_tab_columns
             WHERE owner = :own AND table_name = :t
             ORDER BY column_id";
        using var cmd = new OracleCommand(sql, conn) { BindByName = true };
        cmd.Parameters.Add("own", owner.ToUpperInvariant());
        cmd.Parameters.Add("t", table.ToUpperInvariant());
        using var r = cmd.ExecuteReader();
        var dt = new DataTable(table);
        while (r.Read())
        {
            string col = r.GetString(0);
            string dtp = r.GetString(1);
            int? prec  = r.IsDBNull(2) ? null : (int?)Convert.ToInt32(r.GetDecimal(2));
            int? scale = r.IsDBNull(3) ? null : (int?)Convert.ToInt32(r.GetDecimal(3));
            bool isId  = r.GetString(4) == "YES";
            bool isVt  = r.GetString(5) == "YES";
            if (isId || isVt) continue;
            dt.Columns.Add(col, MapOracleToDotNet(dtp, prec, scale));
        }
        return dt;
    }

    private static string[] LoadPkColumns(OracleConnection conn, string owner, string table)
    {
        const string sql = @"
            SELECT cc.column_name
            FROM   all_constraints c
            JOIN   all_cons_columns cc
                   ON cc.owner = c.owner
                  AND cc.constraint_name = c.constraint_name
                  AND cc.table_name = c.table_name
            WHERE  c.owner = :own AND c.table_name = :tbl AND c.constraint_type = 'P'
            ORDER BY cc.position";
        using var cmd = new OracleCommand(sql, conn) { BindByName = true };
        cmd.Parameters.Add("own", owner.ToUpperInvariant());
        cmd.Parameters.Add("tbl", table.ToUpperInvariant());
        using var r = cmd.ExecuteReader();
        var list = new List<string>();
        while (r.Read()) list.Add(r.GetString(0));
        return list.ToArray();
    }

    private static Type MapOracleToDotNet(string t, int? p, int? s)
    {
        return t switch
        {
            "NUMBER" => (s ?? 0) > 0 ? typeof(decimal)
                       : (p ?? 0) == 0 ? typeof(decimal)
                       : p <= 9 ? typeof(int)
                       : p <= 18 ? typeof(long)
                       : typeof(decimal),
            "FLOAT" => typeof(double),
            "DATE" or "TIMESTAMP" or "TIMESTAMP(6)" => typeof(DateTime),
            "VARCHAR2" or "NVARCHAR2" or "CHAR" or "NCHAR" or "CLOB" or "NCLOB" => typeof(string),
            "BLOB" or "RAW" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    private string? BuildKey(IDictionary<string, object?> values)
    {
        if (_keyColumns.Length == 0) return null;
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

    public void Dispose()
    {
        Flush();
        _buf.Dispose();
    }
}

// --- 사용 예 ---
// var buf = new BulkBuffer(connStr, "APP", "ORDERS", 5000);
// buf.Add(new Dictionary<string,object?> { ["ORDER_ID"]=1, ["CUST_ID"]=10 });
// buf.Flush(); // 지금까지 쌓인 거 즉시 적재
// buf.Dispose();
