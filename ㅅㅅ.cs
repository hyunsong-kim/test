using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using RabbitMQ.Client;

public sealed class RealtimePublisherWithDedup : IDisposable
{
    private readonly IConnection _conn;
    private readonly IModel _ch;
    private readonly IBasicProperties _props;

    private readonly ConcurrentDictionary<string, long> _seen = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
    private readonly Timer _gcTimer;

    private const string Exchange = "bulk.ex";
    private readonly string _routingKey;

    // TTL/청소 설정
    private readonly TimeSpan _dedupTtl;
    private readonly int _maxKeys;

    public RealtimePublisherWithDedup(
        string host = "localhost", string user = "guest", string pass = "guest",
        string routingKey = "bulk",
        TimeSpan? dedupTtl = null, int maxKeys = 1_000_000)
    {
        _routingKey = routingKey;
        _dedupTtl = dedupTtl ?? TimeSpan.FromMinutes(10); // 최근 10분 중복 차단
        _maxKeys = Math.Max(10_000, maxKeys);

        var f = new ConnectionFactory
        {
            HostName = host,
            UserName = user,
            Password = pass,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };
        _conn = f.CreateConnection();
        _ch = _conn.CreateModel();
        _ch.ExchangeDeclare(Exchange, ExchangeType.Direct, durable: true);
        _ch.ConfirmSelect();

        _props = _ch.CreateBasicProperties();
        _props.Persistent = true;
        _props.ContentType = "application/json";

        // 주기적으로 오래된 키 청소
        _gcTimer = new Timer(_ => Cleanup(), null, dueTime: 60_000, period: 60_000);
    }

    /// <summary>
    /// 단일 이벤트 전송. key가 이미 본 적 있으면 false(전송 안 함), 아니면 전송 후 true.
    /// </summary>
    public bool TryPublish(string table, string key, object payload)
    {
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentException("key required");

        var nowTicks = DateTime.UtcNow.Ticks;

        // 1) 멱등키 중복 체크 (프로세스 내)
        // 이미 있으면 false
        if (!_seen.TryAdd(key, nowTicks))
            return false;

        // 2) 메시지 구성
        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new
        {
            Table = table,
            Key = key,
            Payload = payload,
            Ts = DateTime.UtcNow
        }));

        // (선택) 브로커 쪽에도 흔적을 남기고 싶으면 MessageId에 key 설정
        _props.MessageId = key;

        // 3) 발행 + 컨펌
        _ch.BasicPublish(Exchange, _routingKey, mandatory: true, basicProperties: _props, body: body);
        _ch.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));

        return true;
    }

    private void Cleanup()
    {
        // 키 개수가 너무 많거나 TTL 지난 키를 제거
        var now = DateTime.UtcNow;
        var expireBefore = now - _dedupTtl;

        // 개수 상한 초과 시: 대략 절반만 남기도록 빠르게 슬림화
        if (_seen.Count > _maxKeys)
        {
            int target = _maxKeys / 2;
            int removed = 0;
            foreach (var kv in _seen)
            {
                if (removed >= _seen.Count - target) break;
                _seen.TryRemove(kv.Key, out _);
                removed++;
            }
            return;
        }

        // TTL 기반 정리
        foreach (var kv in _seen)
        {
            var when = new DateTime(kv.Value, DateTimeKind.Utc);
            if (when < expireBefore)
                _seen.TryRemove(kv.Key, out _);
        }
    }

    public void Dispose()
    {
        _gcTimer?.Dispose();
        _ch?.Close(); _conn?.Close();
        _ch?.Dispose(); _conn?.Dispose();
    }
}
