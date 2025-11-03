using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using RabbitMQ.Client;

public sealed class RealtimePublisherWithDedup : IDisposable
{
    private readonly IConnection _conn;
    private readonly ConcurrentBag<IModel> _channelPool = new ConcurrentBag<IModel>();
    private readonly IBasicProperties _props;
    private readonly ConcurrentDictionary<string,long> _seen = new(StringComparer.Ordinal);

    private readonly Timer _gcTimer;
    private readonly string _exchange, _routingKey;
    private readonly TimeSpan _dedupTtl;
    private readonly int _maxKeys;

    public RealtimePublisherWithDedup(
        string host="localhost", string user="guest", string pass="guest",
        string exchange="bulk.ex", string routingKey="bulk",
        TimeSpan? dedupTtl=null, int maxKeys=1_000_000, int initialChannels=2)
    {
        _exchange = exchange; _routingKey = routingKey;
        _dedupTtl = dedupTtl ?? TimeSpan.FromMinutes(10);
        _maxKeys = Math.Max(10_000, maxKeys);

        var f = new ConnectionFactory {
            HostName = host, UserName = user, Password = pass,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };
        _conn = f.CreateConnection();

        // 채널 풀 준비
        for (int i=0; i<initialChannels; i++)
            _channelPool.Add(CreateChannel());

        // 공통 프로퍼티(필요 시 메시지마다 복제 가능)
        var chForProps = Borrow();
        try {
            _props = chForProps.CreateBasicProperties();
            _props.Persistent = true;
            _props.ContentType = "application/json";
        } finally { Return(chForProps); }

        // 주기적 dedup 정리
        _gcTimer = new Timer(_ => Cleanup(), null, 60_000, 60_000);
    }

    private IModel CreateChannel()
    {
        var ch = _conn.CreateModel();
        ch.ExchangeDeclare(_exchange, ExchangeType.Direct, durable:true);
        ch.ConfirmSelect(); // 퍼블리셔 컨펌 사용
        return ch;
    }

    private IModel Borrow()
    {
        return _channelPool.TryTake(out var ch) ? ch : CreateChannel();
    }
    private void Return(IModel ch)
    {
        if (ch?.IsOpen == true) _channelPool.Add(ch);
        else ch?.Dispose();
    }

    /// <summary>
    /// 단건 전송. 중복키면 false, 신규면 true (전송 + confirm).
    /// </summary>
    public bool TryPublish(string table, string key, object payload, bool perMessageConfirm = true)
    {
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentException("key required");
        var nowTicks = DateTime.UtcNow.Ticks;

        // 프로세스 내 멱등(최근 키)
        if (!_seen.TryAdd(key, nowTicks)) return false;

        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new {
            Table = table, Key = key, Payload = payload, Ts = DateTime.UtcNow
        }));

        var ch = Borrow();
        try
        {
            var props = _props; // 필요 시 ch.CreateBasicProperties()로 매번 새로 만들어도 OK
            props.MessageId = key; // 브로커 측에도 키 흔적

            ch.BasicPublish(_exchange, _routingKey, mandatory:true, basicProperties:props, body:body);

            if (perMessageConfirm)
                ch.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5)); // 단건 confirm (지연 민감시 ON)
        }
        finally
        {
            Return(ch);
        }
        return true;
    }

    // 배치 컨펌을 원하면, 여러 번 TryPublish(perMessageConfirm:false) 호출 후 아래 호출
    public void FlushConfirms(TimeSpan? timeout = null)
    {
        var ch = Borrow();
        try {
            ch.WaitForConfirmsOrDie(timeout ?? TimeSpan.FromSeconds(5));
        } finally {
            Return(ch);
        }
    }

    private void Cleanup()
    {
        var expire = DateTime.UtcNow - _dedupTtl;
        // 개수 초과 시 급격 슬림화
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
        // TTL 기반 정리
        foreach (var kv in _seen)
        {
            if (new DateTime(kv.Value, DateTimeKind.Utc) < expire)
                _seen.TryRemove(kv.Key, out _);
        }
    }

    public void Dispose()
    {
        _gcTimer?.Dispose();
        while (_channelPool.TryTake(out var ch)) { try { ch.Close(); } catch {} ch.Dispose(); }
        try { _conn?.Close(); } catch {}
        _conn?.Dispose();
    }
}
