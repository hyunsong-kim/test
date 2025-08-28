// Example usage for .NET Framework 4.8 (no Channels), uses BlockingCollection.
// Project references: Oracle.ManagedDataAccess
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ParallelDynamicBulk.Net48;

class Program
{
    static void Main()
    {
        var connStr = "User Id=APP;Password=***;Data Source=//db:1521/ORCLPDB1;Pooling=true;";

        using (var mgr = new ParallelBulkManager(connStr, batchSize: 5000, queueCapacity: 20000))
        {
            // Start producers (optional)
            var p1 = Task.Run(() =>
            {
                for (int i = 1; i <= 200_000; i++)
                {
                    mgr.Add("ORDERS", new Dictionary<string, object?>
                    {
                        ["ORDER_ID"] = i,
                        ["CUST_ID"]  = i % 1000,
                        ["ORDER_DT"] = DateTime.UtcNow
                    });
                }
            });

            var p2 = Task.Run(() =>
            {
                for (int i = 1; i <= 600_000; i++)
                {
                    mgr.Add("ORDER_LINES", new Dictionary<string, object?>
                    {
                        ["ORDER_ID"] = (i + 2) / 3,
                        ["LINE_NO"]  = (i % 5) + 1,
                        ["SKU"]      = $"SKU-{i % 1000}",
                        ["QTY"]      = 1m
                    });
                }
            });

            Task.WaitAll(p1, p2);
            Console.WriteLine("Done. Disposing manager will flush remaining rows.");
        }
    }
}
