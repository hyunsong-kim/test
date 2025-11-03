using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RmqConsumerOracle
{
    public class OrderMsg
    {
        public string Table { get; set; }      // "APP.ORDERS"
        public object Payload { get; set; }    // JSON object with columns
        public string Key { get; set; }        // idempotency key
        public DateTime Ts { get; set; }
    }

    class Program
    {
        // RabbitMQ
        const string EX = "bulk.ex";
        const string Q  = "bulk.q";
        const string DLX = "bulk.dlx";

        // Oracle
        static readonly string ConnStr = "User Id=app;Password=***;Data Source=HOST:1521/SVC";

        // Batching
        const int FlushSize = 5000;      // flush when rows reach this
        const int BatchSize = 3000;      // OracleBulkCopy.BatchSize
        const int PeriodicMs = 1000;     // flush every 1s even if not full

        static readonly ConcurrentDictionary<string, byte> Seen = new(StringComparer.Ordinal);
        static readonly object Sync = new object();

        static DataTable Buffer;
        static DataTable Schema;
        static int Count = 0;

        static System.Timers.Timer Tick;

        static void Main(string[] args)
        {
            using var conn = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest",
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
                DispatchConsumersAsync = true
            }.CreateConnection();

            using var ch = conn.CreateModel();
            ch.BasicQos(0, prefetchCount: 200, global: false); // backpressure

            // Ensure topology
            ch.ExchangeDeclare(EX, ExchangeType.Direct, durable: true);
            ch.ExchangeDeclare(DLX, ExchangeType.Fanout, durable: true);
            ch.QueueDeclare(Q, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object> { ["x-dead-letter-exchange"] = DLX });
            ch.QueueBind(Q, EX, "bulk");

            // Prepare Oracle schema (once)
            using (var oc = new OracleConnection(ConnStr))
            {
                oc.Open();
                Schema = BuildSchema(oc, owner: "APP", table: "ORDERS");
                Buffer = Schema.Clone();
                Buffer.BeginLoadData();
            }

            Tick = new System.Timers.Timer(PeriodicMs);
            Tick.Elapsed += (_, __) => Flush();
            Tick.AutoReset = true;
            Tick.Start();

            // Consumer
            var consumer = new AsyncEventingBasicConsumer(ch);
            consumer.Received += async (model, ea) =>
            {
                try
                {
                    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                    var msg = JsonConvert.DeserializeObject<OrderMsg>(json);

                    // idempotency: dedup at process-level
                    if (!string.IsNullOrEmpty(msg.Key) && !Seen.TryAdd(msg.Key, 0))
                    {
                        ch.BasicAck(ea.DeliveryTag, false);
                        return;
                    }

                    // map to DataTable
                    lock (Sync)
                    {
                        var row = Buffer.NewRow();
                        var dict = JsonConvert.DeserializeObject<Dictionary<string, object>>(msg.Payload.ToString());
                        foreach (var kv in dict)
                            if (Buffer.Columns.Contains(kv.Key))
                                row[kv.Key] = kv.Value ?? DBNull.Value;
                        Buffer.Rows.Add(row);
                        Count++;

                        if (Count >= FlushSize)
                            Flush();
                    }

                    ch.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex.Message);
                    ch.BasicNack(ea.DeliveryTag, false, requeue: false); // send to DLQ
                }
            };

            ch.BasicConsume(Q, autoAck: false, consumer: consumer);
            Console.WriteLine("Consumer started. Press Enter to exit.");
            Console.ReadLine();

            Tick.Stop();
            Flush();
        }

        static void Flush()
        {
            DataTable toSend;
            int rows;

            lock (Sync)
            {
                if (Count == 0) return;
                Buffer.EndLoadData();
                toSend = Buffer;
                rows = Count;

                // reuse
                Buffer.Rows.Clear();
                Buffer.AcceptChanges();
                Count = 0;
                Buffer.BeginLoadData();
            }

            using var conn = new OracleConnection(ConnStr);
            conn.Open();
            using var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
            {
                DestinationTableName = "APP.ORDERS",
                BulkCopyTimeout = 3600,
                BatchSize = Math.Min(BatchSize, rows)
            };
            bulk.WriteToServer(toSend);
        }

        static DataTable BuildSchema(OracleConnection conn, string owner, string table)
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

        static Type MapOracleToDotNet(string oracleType, int? precision, int? scale) => oracleType switch
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
    }
}
