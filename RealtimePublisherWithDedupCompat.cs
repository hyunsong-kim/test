// RealtimePublisherWithDedupCompat.cs
// RabbitMQ.Client v6.x ~ v7.1.2 호환 (동기/비동기, IModel/IChannel, Confirm 옵션 자동 적응)
using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;

public sealed class RealtimePublisherWithDedupCompat : IAsyncDisposable, IDisposable
{
    // ────────────── 상태 ──────────────
    private readonly string _exchange, _routingKey;
    private readonly TimeSpan _dedupTtl;
    private readonly int _maxKeys;

    private readonly ConcurrentDictionary<string,long> _seen = new(StringComparer.Ordinal);
    private readonly ConcurrentBag<object> _channelPool = new(); // IChannel or IModel
    private readonly Timer _gcTimer;

    private object _conn;                    // IConnection
    private bool _isV7 = false;              // v7 API (IChannel/Async) 사용 여부
    private Type _channelType;               // IChannel or IModel 타입
    private Type _basicPropsType;            // v7 BasicProperties 타입 (없으면 null)

    private RealtimePublisherWithDedupCompat(object conn, string exchange, string routingKey,
                                             TimeSpan dedupTtl, int maxKeys,
                                             bool isV7, Type channelType, Type basicPropsType)
    {
        _conn = conn;
        _exchange = exchange;
        _routingKey = routingKey;
        _dedupTtl = dedupTtl;
        _maxKeys = maxKeys;
        _isV7 = isV7;
        _channelType = channelType;
        _basicPropsType = basicPropsType;

        _gcTimer = new Timer(_ => Cleanup(), null, 60_000, 60_000);
    }

    // ────────────── 생성 ──────────────
    public static async Task<RealtimePublisherWithDedupCompat> CreateAsync(
        string host = "localhost", string user = "guest", string pass = "guest",
        string exchange = "bulk.ex", string routingKey = "bulk",
        TimeSpan? dedupTtl = null, int maxKeys = 1_000_000, int initialChannels = 2)
    {
        var factory = new ConnectionFactory {
            HostName = host, UserName = user, Password = pass,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };

        object conn;
        bool isV7 = false;
        // 1) 연결 생성 (v7: CreateConnectionAsync, v6: CreateConnection)
        var createConnAsync = typeof(ConnectionFactory).GetMethod("CreateConnectionAsync", BindingFlags.Instance | BindingFlags.Public);
        if (createConnAsync != null)
        {
            // v7 경로
            var task = (Task)createConnAsync.Invoke(factory, null);
            await task.ConfigureAwait(false);
            // Task<TResult> 의 Result 추출 (IConnection)
            var resultProp = task.GetType().GetProperty("Result");
            conn = resultProp!.GetValue(task);
            isV7 = true;
        }
        else
        {
            // v6 경로
            conn = factory.CreateConnection();
        }

        // 2) 채널 옵션 (v7만)
        object opts = null;
        if (isV7)
        {
            var optType = Type.GetType("RabbitMQ.Client.CreateChannelOptions, RabbitMQ.Client");
            if (optType != null)
            {
                opts = Activator.CreateInstance(optType);
                // opts.PublisherConfirmationsEnabled = true;
                var prop = optType.GetProperty("PublisherConfirmationsEnabled");
                prop?.SetValue(opts, true);
            }
        }

        // 3) 채널 생성 메서드 확인
        var connType = conn.GetType();
        MethodInfo createChannelAsync = null;
        MethodInfo createModel = null;
        Type channelType = null;
        if (isV7)
        {
            createChannelAsync = connType.GetMethod("CreateChannelAsync", new Type[] { opts?.GetType() ?? typeof(object) })
                                ?? connType.GetMethod("CreateChannelAsync", BindingFlags.Public | BindingFlags.Instance, null, Type.EmptyTypes, null);
            channelType = Type.GetType("RabbitMQ.Client.IChannel, RabbitMQ.Client");
        }
        if (createChannelAsync == null)
        {
            createModel = connType.GetMethod("CreateModel", BindingFlags.Public | BindingFlags.Instance, null, Type.EmptyTypes, null);
            channelType = Type.GetType("RabbitMQ.Client.IModel, RabbitMQ.Client");
        }

        // 4) BasicProperties 타입 (v7)
        var basicPropsType = Type.GetType("RabbitMQ.Client.BasicProperties, RabbitMQ.Client"); // v7에 존재

        var self = new RealtimePublisherWithDedupCompat(conn, exchange, routingKey,
                    dedupTtl ?? TimeSpan.FromMinutes(10), Math.Max(10_000, maxKeys),
                    isV7, channelType!, basicPropsType);

        // 5) 토폴로지 선언을 위해 채널 1개 생성
        var ch0 = await self.CreateChannelAsyncInternal(createChannelAsync, createModel, opts).ConfigureAwait(false);
        await self.EnsureExchangeAsync(ch0).ConfigureAwait(false);
        self._channelPool.Add(ch0);

        // 6) 나머지 채널 프리워밍
        int count = Math.Max(1, initialChannels) - 1;
        for (int i = 0; i < count; i++)
        {
            var ch = await self.CreateChannelAsyncInternal(createChannelAsync, createModel, opts).ConfigureAwait(false);
            self._channelPool.Add(ch);
        }

        return self;
    }

    // 채널 생성: v7(CreateChannelAsync) / v6(CreateModel)
    private async Task<object> CreateChannelAsyncInternal(MethodInfo createChannelAsync, MethodInfo createModel, object opts)
    {
        if (_isV7 && createChannelAsync != null)
        {
            object ch;
            var parameters = createChannelAsync.GetParameters().Length == 0 ? null : new object[] { opts };
            var task = (Task)createChannelAsync.Invoke(_conn, parameters);
            await task.ConfigureAwait(false);
            var resultProp = task.GetType().GetProperty("Result");
            ch = resultProp!.GetValue(task);

            // v7: 퍼블리셔 컨펌 옵션으로 이미 활성화됨. 별도 ConfirmSelect 호출 불필요
            return ch;
        }
        else
        {
            // v6
            var ch = createModel.Invoke(_conn, null);
            // v6: ConfirmSelect() 존재 → 선택적으로 호출 가능 (필수는 아님)
            var confirmSel = ch.GetType().GetMethod("ConfirmSelect", BindingFlags.Public | BindingFlags.Instance);
            confirmSel?.Invoke(ch, null);
            return ch;
        }
    }

    // 교환기 선언 (버전별 Async/Sync 적응)
    private async Task EnsureExchangeAsync(object channel)
    {
        if (_isV7)
        {
            var m = _channelType.GetMethod("ExchangeDeclareAsync", new Type[] {
                typeof(string), typeof(string), typeof(bool), typeof(bool), typeof(object)
            });
            if (m != null)
            {
                // (string exchange, string type, bool durable, bool autoDelete, object arguments)
                var task = (Task)m.Invoke(channel, new object[] { _exchange, ExchangeType.Direct, true, false, null });
                await task.ConfigureAwait(false);
                return;
            }
        }
        // v6 Sync 대체
        var mSync = _channelType.GetMethod("ExchangeDeclare", new Type[] {
            typeof(string), typeof(string), typeof(bool), typeof(bool), typeof(object)
        }) ?? _channelType.GetMethod("ExchangeDeclare", new Type[] { typeof(string), typeof(string), typeof(bool) });
        if (mSync != null)
        {
            // 다양한 시그니처 대응
            var ps = mSync.GetParameters();
            if (ps.Length == 3) mSync.Invoke(channel, new object[] { _exchange, ExchangeType.Direct, true });
            else mSync.Invoke(channel, new object[] { _exchange, ExchangeType.Direct, true, false, null });
        }
    }

    private object Borrow()
    {
        if (_channelPool.TryTake(out var ch)) return ch;
        throw new InvalidOperationException("No channel available in pool.");
    }
    private void Return(object ch)
    {
        if (ch == null) return;
        // ch.IsOpen 체크
        var prop = ch.GetType().GetProperty("IsOpen");
        bool isOpen = prop is not null && prop.GetValue(ch) is bool b && b;
        if (isOpen) _channelPool.Add(ch);
        else (ch as IDisposable)?.Dispose();
    }

    // ────────────── 발행 ──────────────
    public async Task<bool> TryPublishAsync(string table, string key, object payload, int confirmTimeoutMs = 5000)
    {
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentException("key required");
        if (!_seen.TryAdd(key, DateTime.UtcNow.Ticks)) return false; // 멱등

        var json = JsonConvert.SerializeObject(new { Table = table, Key = key, Payload = payload, Ts = DateTime.UtcNow });
        var body = Encoding.UTF8.GetBytes(json);

        var ch = Borrow();
        try
        {
            object props = CreateBasicProperties(ch, key);

            if (_isV7)
            {
                // BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body, cancellationToken)
                var m = _channelType.GetMethod("BasicPublishAsync", new Type[] {
                    typeof(string), typeof(string), typeof(bool),
                    _basicPropsType ?? typeof(object), typeof(ReadOnlyMemory<byte>), typeof(CancellationToken)
                }) ?? _channelType.GetMethod("BasicPublishAsync"); // 시그니처 차이 방어
                using var cts = new CancellationTokenSource(confirmTimeoutMs);
                var task = (Task)m.Invoke(ch, new object[] {
                    _exchange, _routingKey, true, props, (ReadOnlyMemory<byte>)body, cts.Token
                });
                await task.ConfigureAwait(false); // 퍼블리셔 컨펌까지 보장됨(v7 옵션 켠 경우)
            }
            else
            {
                // v6: BasicPublish(exchange, routingKey, mandatory, basicProperties, body)
                var m = _channelType.GetMethod("BasicPublish", new Type[] {
                    typeof(string), typeof(string), typeof(bool),
                    typeof(IBasicProperties), typeof(byte[])
                }) ?? _channelType.GetMethod("BasicPublish", new Type[] {
                    typeof(string), typeof(string), typeof(IBasicProperties), typeof(byte[])
                });
                if (m == null) throw new MissingMethodException("BasicPublish not found on IModel.");
                var ps = m.GetParameters();
                if (ps.Length == 5) m.Invoke(ch, new object[] { _exchange, _routingKey, true, props, body });
                else m.Invoke(ch, new object[] { _exchange, _routingKey, props, body });

                // 필요 시 WaitForConfirms(OrDie)
                var wait = _channelType.GetMethod("WaitForConfirmsOrDie", new Type[] { typeof(TimeSpan) })
                           ?? _channelType.GetMethod("WaitForConfirms");
                if (wait != null)
                {
                    if (wait.GetParameters().Length == 0) wait.Invoke(ch, null);
                    else wait.Invoke(ch, new object[] { TimeSpan.FromMilliseconds(confirmTimeoutMs) });
                }
            }
            return true;
        }
        finally
        {
            Return(ch);
        }
    }

    private object CreateBasicProperties(object ch, string messageId)
    {
        // v7: new BasicProperties { ... }
        if (_isV7 && _basicPropsType != null)
        {
            var props = Activator.CreateInstance(_basicPropsType);
            _basicPropsType.GetProperty("ContentType")?.SetValue(props, "application/json");
            var deliveryModeProp = _basicPropsType.GetProperty("DeliveryMode");
            if (deliveryModeProp != null) deliveryModeProp.SetValue(props, (byte)2); // 2=persistent
            _basicPropsType.GetProperty("MessageId")?.SetValue(props, messageId);
            return props;
        }
        // v6: ch.CreateBasicProperties()
        var m = ch.GetType().GetMethod("CreateBasicProperties", BindingFlags.Public | BindingFlags.Instance);
        if (m == null) throw new MissingMethodException("CreateBasicProperties not found.");
        var bp = m.Invoke(ch, null);
        var t = bp.GetType();
        t.GetProperty("ContentType")?.SetValue(bp, "application/json");
        var persistentProp = t.GetProperty("Persistent");
        if (persistentProp != null) persistentProp.SetValue(bp, true);
        var deliveryMode = t.GetProperty("DeliveryMode");
        if (deliveryMode != null) deliveryMode.SetValue(bp, (byte)2);
        t.GetProperty("MessageId")?.SetValue(bp, messageId);
        return bp;
    }

    // ────────────── 정리 ──────────────
    private void Cleanup()
    {
        var expire = DateTime.UtcNow - _dedupTtl;
        if (_seen.Count > _maxKeys)
        {
            int target = _maxKeys/2, removed = 0;
            foreach (var kv in _seen)
            {
                if (removed >= _seen.Count - target) break;
                _seen.TryRemove(kv.Key, out _); removed++;
            }
            return;
        }
        foreach (var kv in _seen)
        {
            if (new DateTime(kv.Value, DateTimeKind.Utc) < expire)
                _seen.TryRemove(kv.Key, out _);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _gcTimer?.Dispose();

        // 채널 닫기
        while (_channelPool.TryTake(out var ch))
        {
            try
            {
                if (_isV7)
                {
                    var closeAsync = ch.GetType().GetMethod("CloseAsync", BindingFlags.Public | BindingFlags.Instance);
                    if (closeAsync != null)
                    {
                        var task = (Task)closeAsync.Invoke(ch, null);
                        await task.ConfigureAwait(false);
                    }
                    var dispAsync = ch.GetType().GetMethod("DisposeAsync", BindingFlags.Public | BindingFlags.Instance);
                    if (dispAsync != null)
                    {
                        var vt = (ValueTask)dispAsync.Invoke(ch, null);
                        await vt.ConfigureAwait(false);
                        continue;
                    }
                }
            } catch { }
            try { (ch as IDisposable)?.Dispose(); } catch { }
        }

        // 연결 닫기
        if (_conn != null)
        {
            try
            {
                var closeAsync = _conn.GetType().GetMethod("CloseAsync", BindingFlags.Public | BindingFlags.Instance);
                if (closeAsync != null)
                {
                    var task = (Task)closeAsync.Invoke(_conn, null);
                    await task.ConfigureAwait(false);
                }
            } catch { }
            try
            {
                var dispAsync = _conn.GetType().GetMethod("DisposeAsync", BindingFlags.Public | BindingFlags.Instance);
                if (dispAsync != null)
                {
                    var vt = (ValueTask)dispAsync.Invoke(_conn, null);
                    await vt.ConfigureAwait(false);
                }
            } catch { }
            try { (_conn as IDisposable)?.Dispose(); } catch { }
            _conn = null;
        }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
