using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;

public sealed class MyRow
{
    public string SrcName { get; init; } = "";
    public decimal Amount { get; init; }
    public DateTime CreatedAt { get; init; }
}

public static class Bulk5k
{
    public static void InsertInBatches(
        IEnumerable<MyRow> source,
        string connStr,
        string destTable,
        int batchSize = 5000)
    {
        using var conn = new OracleConnection(connStr);
        conn.Open();

        using var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
        {
            DestinationTableName = destTable,
            BatchSize = batchSize,
            BulkCopyTimeout = 0,     // 필요시 초 단위로 제한
            NotifyAfter = batchSize  // 진행 로그 간격
        };
        bulk.OracleRowsCopied += (s, e) =>
            Console.WriteLine($"Copied: {e.RowsCopied:N0}");

        // DataTable 스키마(대상 컬럼 타입과 맞추세요)
        var buf = new DataTable();
        buf.Columns.Add("SRC_NAME",  typeof(string));
        buf.Columns.Add("AMOUNT",    typeof(decimal));
        buf.Columns.Add("CREATED_AT",typeof(DateTime));

        int buffered = 0;
        foreach (var r in source)
        {
            buf.Rows.Add(r.SrcName, r.Amount, r.CreatedAt);
            if (++buffered >= batchSize)
            {
                bulk.WriteToServer(buf); // ← 여기서 flush
                buf.Clear();             // 행만 비움(컬럼 유지)
                buffered = 0;
            }
        }

        if (buffered > 0)               // 잔여분 flush
        {
            bulk.WriteToServer(buf);
            buf.Clear();
        }
    }
}
