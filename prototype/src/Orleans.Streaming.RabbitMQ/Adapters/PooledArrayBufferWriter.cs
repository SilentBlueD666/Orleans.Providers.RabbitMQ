using System.Buffers;

namespace Orleans.Streaming.RabbitMQ.Adapters;

/// <summary>
/// IBufferWriter implementation that uses ArrayPool for memory management.
/// </summary>
/// <remarks>
/// Based on Microsoft ArrayBufferWriter : <see href="https://github.com/dotnet/runtime/blob/main/src/libraries/Common/src/System/Buffers/ArrayBufferWriter.cs"/>,
/// but using ArrayPool to rent and return arrays on dispose to reduce allocations.
/// </remarks>
internal sealed class PooledArrayBufferWriter : IBufferWriter<byte>, IDisposable
{
    private const int DefaultInitialBufferSize = 1024; // 1 KB
    private const int MinimumBufferSize = 1; // 1 byte
    private const int BufferGrowthThreshold = 65536; // 64 KB

    private byte[]? _buffer;
    private int _index;
    private bool _disposed;

    /// <summary>
    /// Creates a new <see cref="PooledArrayBufferWriter"/> instance with default initial buffer size.
    /// </summary>
    public PooledArrayBufferWriter()
    {
        InitializeBuffer(DefaultInitialBufferSize);
    }

    /// <summary>
    /// Initializes a new instance of the PooledBufferWriter class with the specified initial buffer capacity.
    /// </summary>
    /// <remarks>If the specified capacity is zero, the buffer is initialized with the minimum required size.
    /// Specifying a larger initial capacity can reduce the need for future buffer resizing if a large amount of data is
    /// expected.</remarks>
    /// <param name="initialCapacity">The initial number of elements the buffer can hold. Must be zero or greater.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="initialCapacity"/> is negative.</exception>
    public PooledArrayBufferWriter(int initialCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(initialCapacity, nameof(initialCapacity));
        InitializeBuffer(initialCapacity);
    }

    private void InitializeBuffer(int size)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(size);
        _index = 0;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PooledArrayBufferWriter));
    }

    /// <summary>
    /// Gets a read-only view of the bytes that have been written to the buffer.
    /// </summary>
    /// <remarks>The returned memory contains only the portion of the buffer that has been written, starting
    /// at the beginning and extending up to the current write position. Accessing this property after the object has
    /// been disposed will throw an exception.</remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the PooledBufferWriter has been disposed.</exception>
    public ReadOnlyMemory<byte> WrittenMemory
    {
        get
        {
            ThrowIfDisposed();
            return _buffer.AsMemory(0, _index);
        }
    }

    public void Advance(int count)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(count, nameof(count));

        if (_index + count > _buffer!.Length)
            throw new InvalidOperationException("Cannot advance beyond buffer capacity.");

        _index += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        return _buffer.AsMemory(_index);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        return _buffer.AsSpan(_index);
    }

    private void CheckAndResizeBuffer(int sizeHint)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint, nameof(sizeHint));

        int currentSize = _buffer!.Length;
        int requiredSize = _index + (sizeHint > 0 ? sizeHint : MinimumBufferSize);

        if (requiredSize > currentSize)
        {
            int newSize = currentSize < BufferGrowthThreshold
                ? Math.Max(requiredSize, currentSize * 2)
                : requiredSize;

            byte[] newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
            Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _index);
            ArrayPool<byte>.Shared.Return(_buffer, true);

            _buffer = newBuffer;
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        if (_buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(_buffer, true);
            _buffer = null;
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }
}