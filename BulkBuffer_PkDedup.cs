// BulkBuffer with PK-based deduplication (auto-load PK columns from Oracle metadata).
// .NET Framework 4.8 compatible. Requires Oracle.ManagedDataAccess.
//
// 특징:
// - 생성자에 OWNER, TABLE을 주면 PK 컬럼 목록을 자동 조회해 _keyColumns에 설정
// - Add() 시 PK 컬럼 값으로 키를 만들어 HashSet으로 중복 차단 (배치 내 기준; Flush 후 클리어)
// - 5,000건(기본)마다 OracleBulkCopy로 INSERT, Flush 후 DataTable은 Clear해서 재사용
//
// 필요 NuGet: Oracle.ManagedDataAccess
// TargetFramework: net48 (또는 net472 등 .NET Framework 계열)
//
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Oracle.ManagedDataAccess.Client;

public sealed class BulkBuffer : IDisposable
{
    private readonly string _connStr;
    private readonly string _owner;
    private readonly string _table;
    private readonly int _batchSize;
    private readonly DataTable _buf;
    private readonly string[] _keyColumns;                 // PK 컬럼명 (대문자)
    private readonly HashSet<string> _seen = new HashSet<string>(StringComparer.Ordinal);
    private int _count = 0;

    public BulkBuffer(string connStr, string owner, string table, int batchSize = 5000)
    {
        _connStr = connStr ?? throw new ArgumentNullException(nameof(connStr));
        _owner   = owner   ?? throw new ArgumentNullException(nameof(owner));
        _table   = table   ?? throw new ArgumentNullException(nameof(table));
        _batchSize = batchSize;

        using (var conn = new OracleConnection(_connStr))
        {
            conn.Open();
            _buf = BuildSchema(conn, _owner, _table);
            _keyColumns = LoadPkColumns(conn, _owner, _table)
                            .Select(c => c.ToUpperInvariant())
                            .ToArray();
        }
    }

    // --- Public API ---
    public void Add(IDictionary<string, object> values)
    {
        // 1) PK 기반 중복 차단 (배치 내)
        var key = BuildKey(values);
        if (key != null && !_seen.Add(key))
            return; // 중복 → 스킵

        // 2) DataRow 채우기 (알려진 컬럼만)
        var row = _buf.NewRow();
        foreach (var kv in values)
        {
            if (_buf.Columns.Contains(kv.Key))
                row[kv.Key] = kv.Value ?? DBNull.Value;
        }
        _buf.Rows.Add(row);

        if (++_count >= _batchSize)
            Flush();
    }

    public void Flush()
    {
        if (_count == 0) return;

        using (var conn = new OracleConnection(_connStr))
        {
            conn.Open();
            using (var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction))
            {
                bulk.DestinationTableName = $"{_owner}.{_table}";
                bulk.BatchSize = _batchSize;
                bulk.BulkCopyTimeout = 0;
                bulk.WriteToServer(_buf);
            }
        }
        _buf.Clear();
        _seen.Clear(); // 배치 내 멱등 → Flush 후 초기화 (전역 멱등 원하면 제거)
        _count = 0;
    }

    public void Dispose()
    {
        Flush();       // 잔여분 반영
        _buf.Dispose();
    }

    // --- Internals ---
    private static DataTable BuildSchema(OracleConnection conn, string owner, string tableName)
    {
        const string sql = @"
            SELECT column_name, data_type, data_precision, data_scale,
                   identity_column, virtual_column
              FROM all_tab_columns
             WHERE owner = :own
               AND table_name = :t
             ORDER BY column_id";

        using (var cmd = new OracleCommand(sql, conn))
        {
            cmd.BindByName = true;
            cmd.Parameters.Add("own", owner.ToUpperInvariant());
            cmd.Parameters.Add("t",   tableName.ToUpperInvariant());

            using (var r = cmd.ExecuteReader())
            {
                var dt = new DataTable(tableName);
                while (r.Read())
                {
                    string colName = r.GetString(0);
                    string dataType = r.GetString(1);
                    int? precision = r.IsDBNull(2) ? (int?)null : Convert.ToInt32(r.GetDecimal(2));
                    int? scale = r.IsDBNull(3) ? (int?)null : Convert.ToInt32(r.GetDecimal(3));
                    bool isIdentity = r.GetString(4) == "YES";
                    bool isVirtual = r.GetString(5) == "YES";
                    if (isIdentity || isVirtual) continue;

                    dt.Columns.Add(colName, MapOracleToDotNet(dataType, precision, scale));
                }
                return dt;
            }
        }
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
            WHERE  c.owner = :own
              AND  c.table_name = :tbl
              AND  c.constraint_type = 'P'
            ORDER BY cc.position";
        using (var cmd = new OracleCommand(sql, conn) { BindByName = true })
        {
            cmd.Parameters.Add("own", owner.ToUpperInvariant());
            cmd.Parameters.Add("tbl", table.ToUpperInvariant());
            var list = new List<string>();
            using (var r = cmd.ExecuteReader())
                while (r.Read()) list.Add(r.GetString(0));
            return list.ToArray();
        }
    }

    private static Type MapOracleToDotNet(string oracleType, int? precision, int? scale)
    {
        switch (oracleType)
        {
            case "NUMBER":
                if ((scale ?? 0) > 0) return typeof(decimal);
                if ((precision ?? 0) == 0) return typeof(decimal);
                if (precision <= 9)  return typeof(int);
                if (precision <= 18) return typeof(long);
                return typeof(decimal);
            case "FLOAT":        return typeof(double);
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP(6)": return typeof(DateTime);
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "CLOB":
            case "NCLOB":       return typeof(string);
            case "BLOB":
            case "RAW":         return typeof(byte[]);
            default:            return typeof(string);
        }
    }

    private string BuildKey(IDictionary<string, object> values)
    {
        if (_keyColumns == null || _keyColumns.Length == 0) return null; // PK 없음 → 중복 체크 생략

        var parts = new string[_keyColumns.Length];
        for (int i = 0; i < _keyColumns.Length; i++)
        {
            var col = _keyColumns[i];
            object v;
            if (!values.TryGetValue(col, out v) || v == null || v is DBNull)
            {
                parts[i] = "<NULL>";
            }
            else if (v is string s)
            {
                parts[i] = s.Trim().ToUpperInvariant(); // 문자열 정규화 정책
            }
            else
            {
                parts[i] = Convert.ToString(v, CultureInfo.InvariantCulture) ?? "";
            }
        }
        return string.Join("|", parts);
    }
}

// --- 간단 사용 예 ---
// var buf = new BulkBuffer(connStr, owner: "APP", table: "ORDERS", batchSize: 5000);
// buf.Add(new Dictionary<string, object> { ["ORDER_ID"]=1, ["CUST_ID"]=10, ["ORDER_DT"]=DateTime.UtcNow });
// ... 여러번 Add(...)
// buf.Dispose(); // 또는 using으로 감싸면 자동 Flush+Dispose
