// BulkBuffer with global instance usage (reusable DataTable).
// .NET Framework 4.8 compatible. Requires Oracle.ManagedDataAccess.

using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;

public class BulkBuffer : IDisposable
{
    private readonly string _connStr;
    private readonly string _tableName;
    private readonly int _batchSize;
    private readonly DataTable _buf;
    private int _count = 0;

    public BulkBuffer(string connStr, string tableName, int batchSize = 5000)
    {
        _connStr = connStr;
        _tableName = tableName;
        _batchSize = batchSize;

        using (var conn = new OracleConnection(_connStr))
        {
            conn.Open();
            _buf = BuildSchema(conn, _tableName);
        }
    }

    private static DataTable BuildSchema(OracleConnection conn, string tableName)
    {
        const string sql = @"
            SELECT column_name, data_type, data_precision, data_scale,
                   identity_column, virtual_column
              FROM user_tab_columns
             WHERE table_name = :t
             ORDER BY column_id";

        using (var cmd = new OracleCommand(sql, conn))
        {
            cmd.BindByName = true;
            cmd.Parameters.Add("t", tableName.ToUpperInvariant());

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

    private static Type MapOracleToDotNet(string oracleType, int? precision, int? scale)
    {
        switch (oracleType)
        {
            case "NUMBER":
                if ((scale ?? 0) > 0) return typeof(decimal);
                if ((precision ?? 0) == 0) return typeof(decimal);
                if (precision <= 9) return typeof(int);
                if (precision <= 18) return typeof(long);
                return typeof(decimal);
            case "FLOAT": return typeof(double);
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP(6)": return typeof(DateTime);
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "CLOB":
            case "NCLOB": return typeof(string);
            case "BLOB":
            case "RAW": return typeof(byte[]);
            default: return typeof(string);
        }
    }

    public void Add(IDictionary<string, object> values)
    {
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
                bulk.DestinationTableName = _tableName;
                bulk.BatchSize = _batchSize;
                bulk.WriteToServer(_buf);
            }
        }
        _buf.Clear();
        _count = 0;
    }

    public void Dispose()
    {
        Flush();
        _buf.Dispose();
    }
}

// 전역 관리 클래스
public static class Buffers
{
    public static BulkBuffer Orders;
    public static BulkBuffer Lines;

    public static void Init(string connStr)
    {
        Orders = new BulkBuffer(connStr, "ORDERS", 5000);
        Lines  = new BulkBuffer(connStr, "ORDER_LINES", 5000);
    }

    public static void DisposeAll()
    {
        Orders?.Dispose();
        Lines?.Dispose();
    }
}

// 예제 사용
class Program
{
    static void Main()
    {
        string connStr = "User Id=APP;Password=***;Data Source=//db:1521/ORCLPDB1;";

        Buffers.Init(connStr);

        // 앱 전역에서 자유롭게 Add 사용
        Buffers.Orders.Add(new Dictionary<string, object>
        {
            ["ORDER_ID"] = 1,
            ["CUST_ID"]  = 10,
            ["ORDER_DT"] = DateTime.UtcNow
        });

        Buffers.Lines.Add(new Dictionary<string, object>
        {
            ["ORDER_ID"] = 1,
            ["LINE_NO"]  = 1,
            ["SKU"]      = "ABC",
            ["QTY"]      = 3m
        });

        Buffers.DisposeAll(); // 프로그램 종료 시
    }
}
