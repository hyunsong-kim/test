using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Oracle.ManagedDataAccess.Client;

namespace BulkInsertKit
{
    /// <summary>
    /// Single-flight + Backpressure + Periodic Flush + Global Cap
    /// - 테이블별 Flush는 항상 1개만 동작(싱글-플라이트)
    /// - Flush 중 추가 호출은 _flushPending=1 로 병합(coalesce)
    /// - DataTable 버퍼 재사용(Rows.Clear), N회마다 축소 리프레시
    /// - 버퍼 하드캡(행 슬롯 세마포어)로 생산자 역압(Backpressure)
    /// - 주기 플러시(타이머)로 묵힘 방지
    /// - 전역 동시 Flush 상한(GlobalCap)로 DB 과부하 방지
    /// .NET Framework 4.8 호환
    /// </summary>
    public sealed class BulkBufferAsync_SingleFlight : IDisposable
    {
        // ====== 전역 동시 Flush 상한 ======
        public static int GlobalMaxParallel { get; private set; } = 3;
        private static SemaphoreSlim GlobalCap = new SemaphoreSlim(GlobalMaxParallel, GlobalMaxParallel);

        public static void ConfigureGlobalParallel(int maxParallel)
        {
            if (maxParallel <= 0) maxParallel = 1;
            // 기존 세마포어 교체(새 인스턴스)
            GlobalMaxParallel = maxParallel;
            GlobalCap = new SemaphoreSlim(GlobalMaxParallel, GlobalMaxParallel);
        }

        // ====== 인스턴스 필드 ======
        private readonly string _connStr, _owner, _table;
        private readonly int _triggerFlushSize;          // Add 중 배치 임계치
        private readonly object _sync = new object();

        private readonly DataTable _schema;              // 스키마(불변)
        private DataTable _buf;                          // 재사용 버퍼
        private int _count;                              // 현재 버퍼 행 수

        private volatile int _disposed;
        private volatile int _flushRunning;              // 0 or 1
        private volatile int _flushPending;              // 0 or 1

        // 중복 컷(선택) — PK 기반 키
        private readonly string[] _keyColumns;
        private readonly HashSet<string> _seen = new HashSet<string>(StringComparer.Ordinal);
        private readonly bool _enableDedup;

        // 주기적 축소(파편화 완화)
        private int _flushSinceRefresh = 0;
        private const int RefreshEvery = 100;            // 100번 Flush마다 축소

        // 버퍼 하드캡: 행 슬롯
        private readonly int _hardCapRows;
        private readonly SemaphoreSlim _rowSlots;

        // 주기 플러시 타이머
        private readonly System.Timers.Timer _tick;
        private readonly int _periodicFlushMs;

        public int BulkCopyTimeoutSeconds { get; set; } = 3600; // 0은 무제한 아님(ODP.NET)
        public int InternalBatchSize { get; set; } = 3000;      // OracleBulkCopy.BatchSize

        public BulkBufferAsync_SingleFlight(
            string connStr,
            string owner,
            string table,
            int triggerFlushSize = 5000,
            bool enableDedup = false,
            int hardCapRows = 50_000,
            int periodicFlushMs = 1000  // 0이면 비활성
        )
        {
            _connStr = connStr;
            _owner = owner;
            _table = table;

            _triggerFlushSize = Math.Max(1, triggerFlushSize);
            _enableDedup = enableDedup;

            _hardCapRows = Math.Max(_triggerFlushSize, hardCapRows);
            _rowSlots = new SemaphoreSlim(_hardCapRows, _hardCapRows);

            _periodicFlushMs = periodicFlushMs;

            using (var conn = new OracleConnection(_connStr))
            {
                conn.Open();
                _schema = BuildSchema(conn, _owner, _table);
                _buf = _schema.Clone();
                _buf.BeginLoadData();
                _keyColumns = LoadPkColumns(conn, _owner, _table);
            }

            if (_periodicFlushMs > 0)
            {
                _tick = new System.Timers.Timer(_periodicFlushMs);
                _tick.AutoReset = true;
                _tick.Elapsed += (s, e) =>
                {
                    try { FlushAsync(); } catch { /* swallow timer exceptions */ }
                };
                _tick.Start();
            }
        }

        /// <summary>Add 한 건. 버퍼 하드캡을 넘어서는 생산은 대기(backpressure)됩니다.</summary>
        public void Add(IDictionary<string, object> values)
        {
            if (Volatile.Read(ref _disposed) != 0)
                throw new ObjectDisposedException(nameof(BulkBufferAsync_SingleFlight));

            // (선택) 중복 컷은 슬롯 점유 전에 판단
            if (_enableDedup)
            {
                var key = BuildKey(values);
                if (key != null && !_seen.Add(key))
                    return; // 중복 컷
            }

            // 버퍼 하드캡 제어: 행 1개당 슬롯 1개
            _rowSlots.Wait();

            bool added = false;
            try
            {
                lock (_sync)
                {
                    var row = _buf.NewRow();
                    foreach (var kv in values)
                    {
                        if (_buf.Columns.Contains(kv.Key))
                            row[kv.Key] = kv.Value ?? DBNull.Value;
                    }
                    _buf.Rows.Add(row);
                    _count++;
                    added = true;

                    if (_count >= _triggerFlushSize)
                        TryStartFlush_NoLock();
                }
            }
            catch
            {
                if (!added) _rowSlots.Release(); // 실패했으면 슬롯 복구
                throw;
            }
        }

        /// <summary>원할 때 수동 Flush 트리거(비동기). 이미 실행 중이면 펜딩 표시만.</summary>
        public Task FlushAsync()
        {
            if (Volatile.Read(ref _disposed) != 0) return Task.CompletedTask;
            lock (_sync) return TryStartFlush_NoLock();
        }

        /// <summary>동기 대기 버전.</summary>
        public void Flush()
        {
            Task t;
            lock (_sync) t = TryStartFlush_NoLock();
            t?.Wait();
        }

        /// <summary>싱글-플라이트: 실행 중이면 펜딩만, 아니면 즉시 시작.</summary>
        private Task TryStartFlush_NoLock()
        {
            if (_count == 0) return Task.CompletedTask;

            if (Interlocked.CompareExchange(ref _flushRunning, 1, 0) != 0)
            {
                // 이미 실행 중 → 끝나자마자 한 번 더 수행
                Volatile.Write(ref _flushPending, 1);
                return Task.CompletedTask;
            }

            // 실행권 획득 → 비동기 태스크 시작
            var task = Task.Run(async () => await FlushWorkerLoopAsync().ConfigureAwait(false));
            return task;
        }

        /// <summary>Flush 루프: 끝나는 시점에 펜딩 있으면 한 번 더 수행.</summary>
        private async Task FlushWorkerLoopAsync()
        {
            try
            {
                // 전역 동시 Flush 상한
                await GlobalCap.WaitAsync().ConfigureAwait(false);
                try
                {
                    do
                    {
                        DataTable toSend;
                        int rows;

                        // 현재 버퍼를 보낼 준비 (EndLoadData)
                        lock (_sync)
                        {
                            if (_count == 0)
                            {
                                // 보낼 것 없음
                                break;
                            }

                            _buf.EndLoadData();
                            toSend = _buf;
                            rows = _count;
                        }

                        // 실제 전송
                        WriteToServer(toSend, rows);

                        // 전송 후 행만 비움 + 재사용 + 슬롯 반환
                        lock (_sync)
                        {
                            toSend.Rows.Clear();
                            toSend.AcceptChanges();
                            _rowSlots.Release(rows);   // 하드캡 슬롯 반납
                            _count = 0;
                            _flushSinceRefresh++;

                            // 주기적 축소 리프레시(파편화 완화)
                            if (_flushSinceRefresh >= RefreshEvery)
                            {
                                toSend.Dispose();
                                _buf = _schema.Clone();
                                _buf.BeginLoadData();
                                _flushSinceRefresh = 0;
                            }
                            else
                            {
                                _buf.BeginLoadData();
                            }
                        }

                        // 펜딩 플래그 읽고 초기화
                        if (Volatile.Read(ref _flushPending) == 1)
                        {
                            Volatile.Write(ref _flushPending, 0);
                            // loop 재실행 (새로 쌓인 버퍼를 한 번 더)
                            continue;
                        }
                        else
                        {
                            // 더 이상 보낼 것 없음
                            break;
                        }

                    } while (true);
                }
                finally
                {
                    GlobalCap.Release();
                }
            }
            finally
            {
                Volatile.Write(ref _flushRunning, 0);
            }
        }

        private void WriteToServer(DataTable dt, int rows)
        {
            using (var conn = new OracleConnection(_connStr))
            {
                conn.Open();
                using (var bulk = new OracleBulkCopy(conn, OracleBulkCopyOptions.UseInternalTransaction))
                {
                    bulk.DestinationTableName = $"{_owner}.{_table}";
                    bulk.BulkCopyTimeout = Math.Max(1, BulkCopyTimeoutSeconds);
                    bulk.BatchSize = Math.Max(1, Math.Min(InternalBatchSize, rows));

                    try
                    {
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        bulk.WriteToServer(dt);
                        sw.Stop();
                        // Console.WriteLine($"[Flush] {_owner}.{_table} rows={rows}, {sw.ElapsedMilliseconds}ms, batch={bulk.BatchSize}");
                    }
                    catch (OracleException ex)
                    {
                        // 필요 시 재시도/분기
                        // Console.Error.WriteLine($"[Flush][ORA-{ex.Number}] {ex.Message}");
                        throw;
                    }
                }
            }
        }

        public void Dispose()
        {
            Volatile.Write(ref _disposed, 1);
            Volatile.Write(ref _flushPending, 0);

            if (_tick != null)
            {
                try { _tick.Stop(); _tick.Dispose(); } catch { }
            }

            // 안전 종료: 남은 것 있으면 한 번 처리 (동기 대기)
            if (Interlocked.CompareExchange(ref _flushRunning, 1, 0) == 0)
            {
                try
                {
                    if (_count > 0) FlushWorkerLoopAsync().GetAwaiter().GetResult();
                }
                finally
                {
                    Volatile.Write(ref _flushRunning, 0);
                }
            }

            lock (_sync)
            {
                _buf?.Dispose();
            }
        }

        // ===== 스키마/PK/키 빌드 =====
        private static DataTable BuildSchema(OracleConnection conn, string owner, string table)
        {
            const string sql = @"
                SELECT column_name, data_type, data_precision, data_scale,
                       identity_column, virtual_column
                FROM   all_tab_columns
                WHERE  owner = :own AND table_name = :t
                ORDER  BY column_id";
            using (var cmd = new OracleCommand(sql, conn) { BindByName = true })
            {
                cmd.Parameters.Add("own", owner.ToUpperInvariant());
                cmd.Parameters.Add("t", table.ToUpperInvariant());

                using (var r = cmd.ExecuteReader())
                {
                    var dt = new DataTable(table);
                    while (r.Read())
                    {
                        var name = r.GetString(0);
                        var typ = r.GetString(1);
                        int? p = r.IsDBNull(2) ? null : (int?)Convert.ToInt32(r.GetDecimal(2));
                        int? s = r.IsDBNull(3) ? null : (int?)Convert.ToInt32(r.GetDecimal(3));
                        var idn = r.GetString(4) == "YES";
                        var virt = r.GetString(5) == "YES";
                        if (idn || virt) continue; // 생성/가상 컬럼 제외
                        dt.Columns.Add(name, MapOracleToDotNet(typ, p, s));
                    }
                    return dt;
                }
            }
        }

        private static string[] LoadPkColumns(OracleConnection conn, string owner, string table)
        {
            const string sql = @"
                SELECT cc.column_name
                FROM   all_constraints c
                JOIN   all_cons_columns cc
                  ON   cc.owner = c.owner
                 AND   cc.constraint_name = c.constraint_name
                 AND   cc.table_name = c.table_name
                WHERE  c.owner = :own
                  AND  c.table_name = :tbl
                  AND  c.constraint_type = 'P'
                ORDER  BY cc.position";
            using (var cmd = new OracleCommand(sql, conn) { BindByName = true })
            {
                cmd.Parameters.Add("own", owner.ToUpperInvariant());
                cmd.Parameters.Add("tbl", table.ToUpperInvariant());
                using (var r = cmd.ExecuteReader())
                {
                    var list = new List<string>();
                    while (r.Read()) list.Add(r.GetString(0));
                    return list.ToArray();
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

        private string BuildKey(IDictionary<string, object> values)
        {
            if (!_enableDedup || _keyColumns == null || _keyColumns.Length == 0) return null;
            var parts = new string[_keyColumns.Length];
            for (int i = 0; i < _keyColumns.Length; i++)
            {
                var col = _keyColumns[i];
                object v;
                if (values == null || !values.TryGetValue(col, out v) || v == null || v is DBNull)
                    parts[i] = "<NULL>";
                else if (v is string s)
                    parts[i] = s.Trim().ToUpperInvariant();
                else
                    parts[i] = Convert.ToString(v, CultureInfo.InvariantCulture) ?? "";
            }
            return string.Join("|", parts);
        }
    }
}
