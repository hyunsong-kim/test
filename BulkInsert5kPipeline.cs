using System.Threading.Channels;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Threading.Tasks;

public sealed class MyRow
{
    public string SrcName { get; init; } = "";
    public decimal Amount { get; init; }
    public DateTime CreatedAt { get; init; }
}

public static class Bulk5kPipeline
{
    public static async Task ConsumerAsync(
        ChannelReader<MyRow> reader,
        string connStr,
        string destTable,
        int batchSize = 5000)
    {
        using var conn = new OracleConnection(connStr);
        await conn.OpenAsync();

        using var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
        {
            DestinationTableName = destTable,
            BatchSize = batchSize,
            BulkCopyTimeout = 0,
            NotifyAfter = batchSize
        };

        var buf = new DataTable();
        buf.Columns.Add("SRC_NAME",  typeof(string));
        buf.Columns.Add("AMOUNT",    typeof(decimal));
        buf.Columns.Add("CREATED_AT",typeof(DateTime));

        int buffered = 0;
        await foreach (var r in reader.ReadAllAsync())
        {
            buf.Rows.Add(r.SrcName, r.Amount, r.CreatedAt);
            if (++buffered >= batchSize)
            {
                bulk.WriteToServer(buf); // flush
                buf.Clear();
                buffered = 0;
            }
        }
        if (buffered > 0)
        {
            bulk.WriteToServer(buf);
            buf.Clear();
        }
    }
}
