// BulkBuffer with simple in-memory deduplication by key columns (batch-scoped).
// ..NET Framework 4.8 compatible. Requires Oracle.ManagedDataAccess.
//
// How it works:
// - Provide key column names in the constructor (e.g., new[] {"ORDER_ID","LINE_NO"}).
// - Each Add() computes a canonical key from those columns and skips duplicates.
// - Keys are cleared on Flush(), so dedup scope is "within a batch".
// - Keep it simple: no locks (single-thread Add assumed). Add a lock if multi-thread is needed.

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Oracle.ManagedDataAccess.Client;

public class BulkBuffer : IDisposable
{
    private readonly string _connStr;
    private readonly string _tableName;
    private readonly int _batchSize;
    private readonly string[] _keyColumns; // dedup keys (DB column names)
    private readonly DataTable _buf;
    private int _count = 0;

    // in-memory key set for batch-scoped dedup
    private readonly HashSet<string> _keys = new HashSet<string>(StringComparer.Ordinal);

    public BulkBuffer(string connStr, string tableName, int batchSize = 5000, params string[] keyColumns)
    {
        _connStr = connStr;
        _tableName = tableName;
        _batchSize = batchSize;
        _keyColumns = keyColumns ?? Array.Empty<string>();

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

    // Canonicalize key values to reduce accidental mismatches (trim/case-fold for strings)
    private string BuildKey(IDictionary<string, object> values)
    {
        if (_keyColumns == null || _keyColumns.Length == 0) return Guid.NewGuid().ToString("N"); // no dedup configured

        var parts = new string[_keyColumns.Length];
        for (int i = 0; i < _keyColumns.Length; i++)
        {
            var col = _keyColumns[i];
            object val;
            if (!values.TryGetValue(col, out val) || val == null || val is DBNull)
            {
                parts[i] = "<NULL>";
                continue;
            }

            if (val is string s)
            {
                parts[i] = s.Trim().ToUpperInvariant();
            }
            else
            {
                parts[i] = Convert.ToString(val, System.Globalization.CultureInfo.InvariantCulture) ?? "";
            }
        }
        // '|' is a safe delimiter for most keys; change if it can appear in your data after canonicalization
        return string.Join("|", parts);
    }

    public void Add(IDictionary<string, object> values)
    {
        // dedup first
        var key = BuildKey(values);
        if (_keyColumns.Length > 0 && !_keys.Add(key))
        {
            // duplicate within the current batch -> skip
            return;
        }

        // then fill a DataRow
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
        _keys.Clear(); // dedup scope: within batch -> reset keys after flush
        _count = 0;
    }

    public void Dispose()
    {
        Flush();
        _buf.Dispose();
    }
}

// Example usage:
class Program
{
    static void Main()
    {
        string connStr = "User Id=APP;Password=***;Data Source=//db:1521/ORCLPDB1;";

        // ORDERS: dedupe by (ORDER_ID)
        using (var orders = new BulkBuffer(connStr, "ORDERS", 5000, "ORDER_ID"))
        // ORDER_LINES: dedupe by (ORDER_ID, LINE_NO)
        using (var lines  = new BulkBuffer(connStr, "ORDER_LINES", 5000, "ORDER_ID", "LINE_NO"))
        {
            // duplicates inside same batch will be skipped
            orders.Add(new Dictionary<string, object> { ["ORDER_ID"]=1, ["CUST_ID"]=10, ["ORDER_DT"]=DateTime.UtcNow });
            orders.Add(new Dictionary<string, object> { ["ORDER_ID"]=1, ["CUST_ID"]=11, ["ORDER_DT"]=DateTime.UtcNow }); // duplicate -> skipped

            lines.Add(new Dictionary<string, object> { ["ORDER_ID"]=1, ["LINE_NO"]=1, ["SKU"]="A", ["QTY"]=1m });
            lines.Add(new Dictionary<string, object> { ["ORDER_ID"]=1, ["LINE_NO"]=1, ["SKU"]="B", ["QTY"]=2m }); // duplicate -> skipped
        }
    }
}
