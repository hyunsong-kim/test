// Parallel bulk loader with dynamic DataTable schemas per table.
// Requires: Oracle.ManagedDataAccess.Core (ODP.NET Core)
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace ParallelDynamicBulk
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

            using var cmd = new OracleCommand(sql, conn) { BindByName = true };
            cmd.Parameters.Add("t", tableName.ToUpperInvariant());
            using var r = cmd.ExecuteReader();

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

    public sealed class DynamicTableWorker : IAsyncDisposable
    {
        private readonly string _connStr;
        private readonly string _tableName;
        private readonly int _batchSize;
        private readonly Channel<IDictionary<string, object?>> _ch;
        private readonly Task _runner;

        private readonly DataTable _schema;
        private readonly DataTable _buf;
        private readonly Dictionary<string, DataColumn> _colMap;
        private int _buffered;

        public ChannelWriter<IDictionary<string, object?>> Writer => _ch.Writer;

        public DynamicTableWorker(string connStr, string tableName, int batchSize, DataTable schema)
        {
            _connStr = connStr;
            _tableName = tableName;
            _batchSize = batchSize;

            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _buf = _schema.Clone();

            _colMap = _buf.Columns.Cast<DataColumn>()
                .ToDictionary(c => c.ColumnName.ToUpperInvariant(), c => c);

            _ch = Channel.CreateBounded<IDictionary<string, object?>>(new BoundedChannelOptions(batchSize * 4)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });

            _runner = Task.Run(RunAsync);
        }

        private async Task RunAsync()
        {
            using var conn = new OracleConnection(_connStr);
            await conn.OpenAsync().ConfigureAwait(false);

            using var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction)
            {
                DestinationTableName = _tableName,
                BatchSize = _batchSize,
                BulkCopyTimeout = 0,
                NotifyAfter = _batchSize
            };

            bulk.OracleRowsCopied += (s, e) =>
            {
                if (e.RowsCopied % _batchSize == 0)
                    Console.WriteLine($"[{_tableName}] Copied {e.RowsCopied:N0} rows...");
            };

            await foreach (var values in _ch.Reader.ReadAllAsync().ConfigureAwait(false))
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

        private void AppendRow(IDictionary<string, object?> values)
        {
            var dr = _buf.NewRow();
            foreach (var kv in values)
            {
                if (_colMap.TryGetValue(kv.Key.ToUpperInvariant(), out var col))
                    dr[col] = Coerce(kv.Value, col.DataType);
            }
            _buf.Rows.Add(dr);
        }

        private static object Coerce(object? v, Type targetType)
        {
            if (v == null) return DBNull.Value;
            if (v is DBNull) return DBNull.Value;

            var vt = v.GetType();
            if (targetType.IsAssignableFrom(vt)) return v;

            try
            {
                if (targetType == typeof(string)) return v.ToString() ?? "";
                if (targetType == typeof(int)) return Convert.ToInt32(v);
                if (targetType == typeof(long)) return Convert.ToInt64(v);
                if (targetType == typeof(decimal)) return Convert.ToDecimal(v);
                if (targetType == typeof(double)) return Convert.ToDouble(v);
                if (targetType == typeof(DateTime)) return Convert.ToDateTime(v);
                if (targetType == typeof(byte[]))
                {
                    if (v is Guid g) return g.ToByteArray();
                    if (v is string s && Guid.TryParse(s, out var gg)) return gg.ToByteArray();
                }
                return Convert.ChangeType(v, targetType);
            }
            catch
            {
                // If coercion fails, store as NULL. You may log or throw instead.
                return DBNull.Value;
            }
        }

        public async ValueTask DisposeAsync()
        {
            _ch.Writer.TryComplete();
            await _runner.ConfigureAwait(false);
            _buf.Dispose();
        }
    }

    public sealed class ParallelBulkManager : IAsyncDisposable
    {
        private readonly string _connStr;
        private readonly int _batchSize;
        private readonly ConcurrentDictionary<string, DynamicTableWorker> _workers = new();
        private readonly ConcurrentDictionary<string, DataTable> _schemaCache = new();

        public ParallelBulkManager(string connStr, int batchSize = 5000)
        {
            _connStr = connStr;
            _batchSize = batchSize;
        }

        private DynamicTableWorker GetOrCreateWorker(string table)
        {
            return _workers.GetOrAdd(table, t =>
            {
                var schema = _schemaCache.GetOrAdd(t, key =>
                {
                    using var conn = new OracleConnection(_connStr);
                    conn.Open();
                    return OracleSchema.BuildSchema(conn, key);
                });
                return new DynamicTableWorker(_connStr, t, _batchSize, schema);
            });
        }

        public ValueTask AddAsync(string table, IDictionary<string, object?> values)
            => GetOrCreateWorker(table).Writer.WriteAsync(values);

        public async Task AddRangeAsync(string table, IEnumerable<IDictionary<string, object?>> rows)
        {
            var writer = GetOrCreateWorker(table).Writer;
            foreach (var r in rows)
                await writer.WriteAsync(r);
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var w in _workers.Values)
                await w.DisposeAsync();
            _workers.Clear();

            foreach (var s in _schemaCache.Values)
                s.Dispose();
            _schemaCache.Clear();
        }
    }
}
