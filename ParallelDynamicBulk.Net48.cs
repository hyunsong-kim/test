// Parallel bulk loader with dynamic DataTable schemas per table (NET48-friendly, no Channels).
// Requires: Oracle.ManagedDataAccess (ODP.NET for .NET Framework)
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace ParallelDynamicBulk.Net48
{
    public static class OracleSchema
    {
        // Build DataTable schema from Oracle metadata (skips identity/virtual columns)
        public static DataTable BuildSchema(OracleConnection conn, string tableName)
        {
            const string sql = @"
            SELECT column_name, data_type, data_length, data_precision, data_scale,
                   identity_column, virtual_column
              FROM user_tab_columns
             WHERE table_name = :t
             ORDER BY column_id";

            using (var cmd = new OracleCommand(sql, conn) { BindByName = true })
            {
                cmd.Parameters.Add("t", tableName.ToUpperInvariant());
                using (var r = cmd.ExecuteReader())
                {
                    var dt = new DataTable { TableName = tableName };
                    while (r.Read())
                    {
                        var colName   = r.GetString(0);
                        var dataType  = r.GetString(1);
                        var precision = r.IsDBNull(3) ? (int?)null : Convert.ToInt32(r.GetDecimal(3));
                        var scale     = r.IsDBNull(4) ? (int?)null : Convert.ToInt32(r.GetDecimal(4));
                        var isIdentity= r.GetString(5) == "YES";
                        var isVirtual = r.GetString(6) == "YES";

                        if (isIdentity || isVirtual) continue; // Let DB populate those

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
                    if ((precision ?? 0) == 0) return typeof(decimal); // unspecified -> decimal
                    if (precision <= 9)  return typeof(int);
                    if (precision <= 18) return typeof(long);
                    return typeof(decimal);
                case "FLOAT":        return typeof(double);
                case "DATE":         return typeof(DateTime);
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
    }

    // Worker per table: bounded BlockingCollection for producer/consumer
    public sealed class DynamicTableWorker : IDisposable
    {
        private readonly string _connStr;
        private readonly string _tableName;
        private readonly int _batchSize;
        private readonly BlockingCollection<IDictionary<string, object?>> _queue;
        private readonly Task _runner;

        private readonly DataTable _schema;
        private readonly DataTable _buf;
        private readonly Dictionary<string, DataColumn> _colMap;
        private int _buffered;

        public DynamicTableWorker(string connStr, string tableName, int batchSize, DataTable schema, int queueCapacity)
        {
            _connStr = connStr;
            _tableName = tableName;
            _batchSize = batchSize;

            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _buf = _schema.Clone();
            _colMap = _buf.Columns.Cast<DataColumn>()
                .ToDictionary(c => c.ColumnName.ToUpperInvariant(), c => c);

            _queue = new BlockingCollection<IDictionary<string, object?>>(boundedCapacity: queueCapacity);
            _runner = Task.Factory.StartNew(Run, TaskCreationOptions.LongRunning);
        }

        // Enqueue one item (blocks if queue is full -> backpressure)
        public void Enqueue(IDictionary<string, object?> values)
        {
            if (!_queue.IsAddingCompleted)
                _queue.Add(values);
        }

        // Enqueue many
        public void EnqueueRange(IEnumerable<IDictionary<string, object?>> rows)
        {
            foreach (var r in rows) Enqueue(r);
        }

        private void Run()
        {
            using (var conn = new OracleConnection(_connStr))
            {
                conn.Open();

                using (var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
                {
                    DestinationTableName = _tableName,
                    BatchSize = _batchSize,
                    BulkCopyTimeout = 0,
                    NotifyAfter = _batchSize
                })
                {
                    foreach (var values in _queue.GetConsumingEnumerable())
                    {
                        AppendRow(values);
                        if (++_buffered >= _batchSize)
                        {
                            bulk.WriteToServer(_buf);
                            _buf.Clear();
                            _buffered = 0;
                        }
                    }

                    if (_buffered > 0)
                    {
                        bulk.WriteToServer(_buf);
                        _buf.Clear();
                        _buffered = 0;
                    }
                }
            }
        }

        private void AppendRow(IDictionary<string, object?> values)
        {
            var dr = _buf.NewRow();
            foreach (var kv in values)
            {
                DataColumn col;
                if (_colMap.TryGetValue(kv.Key.ToUpperInvariant(), out col))
                    dr[col] = Coerce(kv.Value, col.DataType);
            }
            _buf.Rows.Add(dr);
        }

        private static object Coerce(object v, Type targetType)
        {
            if (v == null) return DBNull.Value;
            if (v is DBNull) return DBNull.Value;

            var vt = v.GetType();
            if (targetType.IsAssignableFrom(vt)) return v;

            try
            {
                if (targetType == typeof(string))  return v.ToString() ?? "";
                if (targetType == typeof(int))     return Convert.ToInt32(v);
                if (targetType == typeof(long))    return Convert.ToInt64(v);
                if (targetType == typeof(decimal)) return Convert.ToDecimal(v);
                if (targetType == typeof(double))  return Convert.ToDouble(v);
                if (targetType == typeof(DateTime))return Convert.ToDateTime(v);
                if (targetType == typeof(byte[]))
                {
                    var g = v as Guid?;
                    if (g.HasValue) return g.Value.ToByteArray();
                    var s = v as string;
                    Guid gg;
                    if (s != null && Guid.TryParse(s, out gg)) return gg.ToByteArray();
                }
                return Convert.ChangeType(v, targetType);
            }
            catch
            {
                return DBNull.Value;
            }
        }

        public void Complete()
        {
            _queue.CompleteAdding();
        }

        public void Dispose()
        {
            Complete();
            try { _runner.Wait(); } catch { /* swallow */ }
            _queue.Dispose();
            _buf.Dispose();
        }
    }

    public sealed class ParallelBulkManager : IDisposable
    {
        private readonly string _connStr;
        private readonly int _batchSize;
        private readonly int _queueCapacity;
        private readonly ConcurrentDictionary<string, DynamicTableWorker> _workers = new ConcurrentDictionary<string, DynamicTableWorker>();
        private readonly ConcurrentDictionary<string, DataTable> _schemaCache = new ConcurrentDictionary<string, DataTable>();

        public ParallelBulkManager(string connStr, int batchSize = 5000, int queueCapacity = 20000)
        {
            _connStr = connStr;
            _batchSize = batchSize;
            _queueCapacity = queueCapacity;
        }

        private DynamicTableWorker GetOrCreateWorker(string table)
        {
            return _workers.GetOrAdd(table, t =>
            {
                var schema = _schemaCache.GetOrAdd(t, key =>
                {
                    using (var conn = new OracleConnection(_connStr))
                    {
                        conn.Open();
                        return OracleSchema.BuildSchema(conn, key);
                    }
                });
                return new DynamicTableWorker(_connStr, t, _batchSize, schema, _queueCapacity);
            });
        }

        public void Add(string table, IDictionary<string, object?> values)
            => GetOrCreateWorker(table).Enqueue(values);

        public void AddRange(string table, IEnumerable<IDictionary<string, object?>> rows)
            => GetOrCreateWorker(table).EnqueueRange(rows);

        public void Dispose()
        {
            foreach (var w in _workers.Values) w.Dispose();
            _workers.Clear();

            foreach (var s in _schemaCache.Values) s.Dispose();
            _schemaCache.Clear();
        }
    }
}
