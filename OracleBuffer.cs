
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

public sealed class OracleBuffer<T> : IDisposable
{
    private readonly object _gate = new object();

    private readonly Func<List<T>, Task> _writer;
    private readonly int _countThreshold;
    private readonly bool _autoFlush;

    private readonly List<T> _buf = new List<T>();
    private bool _stopping = false;
    private bool _disposed = false;

    public OracleBuffer(Func<List<T>, Task> writer, int countThreshold = 5000, bool autoFlush = true)
    {
        _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        _countThreshold = countThreshold;
        _autoFlush = autoFlush;
    }

    public async Task AddAsync(T item)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(OracleBuffer<T>));

        bool thresholdHit = false;

        lock (_gate)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OracleBuffer<T>));

            _buf.Add(item);

            if (_autoFlush && !_stopping && _buf.Count >= _countThreshold)
                thresholdHit = true;
        }

        if (thresholdHit)
            await FlushNowAsync().ConfigureAwait(false);
    }

    public async Task FlushNowAsync()
    {
        List<T> batch;

        lock (_gate)
        {
            if (_disposed) return;
            if (_buf.Count == 0) return;

            batch = new List<T>(_buf);
            _buf.Clear();
        }

        await _writer(batch).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;

        _stopping = true;

        // 마지막 잔여 flush
        FlushNowAsync().GetAwaiter().GetResult();

        _disposed = true;
    }
}
