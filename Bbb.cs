// RealtimePublisherWithDedupV7.cs
// RabbitMQ.Client 7.x 전용 (예: 7.1.2)
// - 연결/채널: CreateConnectionAsync / CreateChannelAsync
// - 퍼블리셔 컨펌: CreateChannelOptions.PublisherConfirmationsEnabled = true
// - 단건 전송 TryPublishAsync(table, key, payload)
// - 키 중복 방지(프로세스 내 멱등) + 채널 풀 + 비동기 종료

using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;

public sealed class RealtimePublisherWithDedupV7 : IAsyncDisposable
{
    private readonly IConnection _conn;
    private readonly ConcurrentBag<IChannel> _pool = new();
    private readonly ConcurrentDictionary<string,long> _seen = new(StringComparer.Ordinal);

    private readonly string _exchange;
    private readonly string _routingKey;
    private readonly TimeSpan _dedupTtl;
    private readonly int _maxKeys;
    private readonly Timer _gcTimer;

    private readonly CreateChannelOptions _chanOpts;

    private RealtimePublisherWithDedupV7(
        IConnection conn, string exchange, string routingKey,
        TimeSpan dedupTtl, int maxKeys, int initialChannels)
    {
        _conn = conn;
        _exchange = exchange;
        _routingKey = routingKey;
        _dedupTtl = dedupTtl;
        _maxKeys = maxKeys;

        _chanOpts = new CreateChannelOptions
        {
            PublisherConfirmationsEnabled = true
            // 필요 시: PublisherConfirmationTrackingEnabled = false
        };

        _gcTimer = new Timer(_ => Cleanup(), null, 60_000, 60_000);
    }

    public static async Task<RealtimePublisherWithDedupV7> CreateAsync(
        string host="localhost", string user="guest", string pass="guest",
        string exchange="bulk.ex", string routingKey="bulk",
        TimeSpan? dedupTtl=null, int maxKeys=1_000_000, int initialChannels=2)
    {
        var f = new ConnectionFactory
        {
            HostName = host,
            UserName = user,
            Password = pass,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };

        var conn = await f.CreateConnectionAsync().ConfigureAwait(false);
        var self = new RealtimePublisherWithDedupV7(
            conn, exchange, routingKey,
            dedupTtl ?? TimeSpan.FromMinutes(10),
            Math.Max(10_000, maxKeys),
            Math.Max(1, initialChannels)
        );

        // 토폴로지 선언(교환기). 필요하면 큐/바인딩도 여기서.
        var ch0 = await conn.CreateChannelAsync(self._chanOpts).ConfigureAwait(false);
        await ch0.ExchangeDeclareAsync(exchange, ExchangeType.Direct, durable: true).ConfigureAwait(false);
        self._pool.Add(ch0);

        // 나머지 채널 프리워밍
        for (int i = 1; i < initialChannels; i++)
            self._pool.Add(await conn.CreateChannelAsync(self._chanOpts).ConfigureAwait(false));

        return self;
    }

    private IChannel Borrow() =>
        _pool.TryTake(out var ch) ? ch : throw new InvalidOperationException("No channel available");

    private void Return(IChannel ch)
    {
        if (ch != null && ch.IsOpen) _pool.Add(ch);
        else ch?.Dispose();
    }

    /// <summary>
    /// 단건 전송. 중복키면 false (전송 안 함).
    /// BasicPublishAsync를 await하면 퍼블리셔 컨펌(ack/nack)까지 보장됨(옵션 활성화 시).
    /// </summary>
    public async Task<bool> TryPublishAsync(
        string table, string key, object payload, int confirmTimeoutMs = 5000)
    {
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentException("key required");

        // 프로세스 내 멱등(최근 키)
        if (!_seen.TryAdd(key, DateTime.UtcNow.Ticks)) return false;

        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new
        {
            Table = table,
            Key = key,
            Payload = payload,
            Ts = DateTime.UtcNow
        }));

        var props = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = 2, // persistent
            MessageId = key
        };

        var ch = Borrow();
        try
        {
            using var cts = new CancellationTokenSource(confirmTimeoutMs);
            await ch.BasicPublishAsync(
                exchange: _exchange,
                routingKey: _routingKey,
                mandatory: true,
                basicProperties: props,
                body: body,
                cancellationToken: cts.Token
            ).ConfigureAwait(false); // 퍼블리셔 컨펌 활성화 상태면 여기서 ack/nack까지 완료
            return true;
        }
        finally
        {
            Return(ch);
        }
    }

    private void Cleanup()
    {
        var expire = DateTime.UtcNow - _dedupTtl;

        // 개수 상한 초과 시 급 슬림화
        if (_seen.Count > _maxKeys)
        {
            int target = _maxKeys / 2, removed = 0;
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

    public async ValueTask DisposeAsync()
    {
        _gcTimer?.Dispose();

        while (_pool.TryTake(out var ch))
        {
            try { await ch.CloseAsync().ConfigureAwait(false); } catch {}
            try { await ch.DisposeAsync().ConfigureAwait(false); } catch {}
        }
        try { await _conn.CloseAsync().ConfigureAwait(false); } catch {}
        try { await _conn.DisposeAsync().ConfigureAwait(false); } catch {}
    }
}
