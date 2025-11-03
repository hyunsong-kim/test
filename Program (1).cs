using System;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace RmqProducer
{
    public class OrderMsg
    {
        public string Table { get; set; }      // e.g., "APP.ORDERS"
        public object Payload { get; set; }    // anonymous object with columns
        public string Key { get; set; }        // idempotency key (e.g., PK composite "ORD|LINE")
        public DateTime Ts { get; set; } = DateTime.UtcNow;
    }

    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest",
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
                DispatchConsumersAsync = true
            };

            const string EX = "bulk.ex";
            const string Q  = "bulk.q";
            const string DLX = "bulk.dlx";
            const string DLQ = "bulk.dlq";

            using var conn = factory.CreateConnection();
            using var ch = conn.CreateModel();

            // Topology (idempotent declarations)
            ch.ExchangeDeclare(EX, ExchangeType.Direct, durable: true);
            ch.ExchangeDeclare(DLX, ExchangeType.Fanout, durable: true);
            ch.QueueDeclare(DLQ, durable: true, exclusive: false, autoDelete: false);
            ch.QueueBind(DLQ, DLX, "");

            ch.QueueDeclare(Q, durable: true, exclusive: false, autoDelete: false,
                arguments: new System.Collections.Generic.Dictionary<string, object>
                {
                    ["x-dead-letter-exchange"] = DLX
                });
            ch.QueueBind(Q, EX, "bulk");

            ch.ConfirmSelect(); // publisher confirms

            // Example publish 10 messages
            var props = ch.CreateBasicProperties();
            props.Persistent = true; // survive broker restart

            for (int i = 1; i <= 10; i++)
            {
                var msg = new OrderMsg
                {
                    Table = "APP.ORDERS",
                    Key = $"{1000 + i}|{1}",
                    Payload = new
                    {
                        ORD_ID = 1000 + i,
                        LINE_NO = 1,
                        ITEM = "A",
                        QTY = i
                    }
                };

                var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(msg));
                ch.BasicPublish(EX, "bulk", mandatory: true, basicProperties: props, body: body);

                ch.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
                Console.WriteLine($"Published: {msg.Key}");
                Thread.Sleep(50);
            }

            Console.WriteLine("Done.");
        }
    }
}
