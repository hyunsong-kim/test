// Example usage of ParallelDynamicBulk library.
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ParallelDynamicBulk;

class Program
{
    static async Task Main()
    {
        var connStr = "User Id=APP;Password=***;Data Source=//db:1521/ORCLPDB1;Pooling=true;";

        await using var mgr = new ParallelBulkManager(connStr, batchSize: 5000);

        // Producer for ORDERS
        var p1 = Task.Run(async () =>
        {
            for (int i = 1; i <= 200_000; i++)
            {
                await mgr.AddAsync("ORDERS", new Dictionary<string, object?>
                {
                    ["ORDER_ID"] = i,
                    ["CUST_ID"]  = i % 1000,
                    ["ORDER_DT"] = DateTime.UtcNow
                });
            }
        });

        // Producer for ORDER_LINES
        var p2 = Task.Run(async () =>
        {
            for (int i = 1; i <= 600_000; i++)
            {
                await mgr.AddAsync("ORDER_LINES", new Dictionary<string, object?>
                {
                    ["ORDER_ID"] = (i + 2) / 3,
                    ["LINE_NO"]  = (i % 5) + 1,
                    ["SKU"]      = $"SKU-{i % 1000}",
                    ["QTY"]      = 1m
                });
            }
        });

        await Task.WhenAll(p1, p2);

        Console.WriteLine("Done. Disposing manager will flush remaining rows (if any).");
    }
}
