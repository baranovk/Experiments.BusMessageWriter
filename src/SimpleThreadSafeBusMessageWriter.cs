using System.Runtime.CompilerServices;

namespace Experiments.BusMessageWriter
{
    /// <summary>
    /// SimpleThreadSafeBusMessageWriter - is a simple realization of thread-safe 
    /// BusMessageWriter with buffering support.
    /// 
    /// We can further improve it, for example by using separate producer thread for
    /// sending messages to bus (for example, we can use in some manner BlockingCollection<T> wrapping
    /// ConcurrentQueue)
    /// </summary>
    public class SimpleThreadSafeBusMessageWriter : IDisposable, IAsyncDisposable
    {
        #region Fields

        private bool _disposed;
        private readonly int _flushBufferThresholdBytes;
        private readonly IBusConnection _busConnection;
        private readonly MemoryStream _buffer = new();
        private readonly SemaphoreSlim _writeMutex = new(1);

        #endregion

        #region Constructors

        public SimpleThreadSafeBusMessageWriter(
            int flushBufferThresholdBytes,
            IBusConnection busConnection)
        {
            _flushBufferThresholdBytes = flushBufferThresholdBytes;
            _busConnection = busConnection;
            _buffer = new(flushBufferThresholdBytes);
        }

        #endregion

        #region Public Methods

        public async Task SendMessageAsync(
            byte[] nextMessage,
            // for example, if some producer can tolerate loosing messages
            // but can not tolerate continuous blocking
            CancellationToken cancellationToken = default)
        {
            await SendInternal(nextMessage, false, cancellationToken);
        }

        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            await SendInternal(null, true, cancellationToken);
        }

        public void Flush()
        {
            FlushAsync().Wait();
        }

        public void Dispose()
        {
            // Dispose is not thread safe
            // Leaving dispose synchronization to BusMessageWriter client

            // I do not implement Dispose(disposing) pattern because class
            // doesn't use unmanaged resources directly, so doesn't need finalizer
            if (_disposed)
            {
                return;
            }

            try
            {
                Flush();
            }
            finally
            {
                _writeMutex.Dispose();
                _disposed = true;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                await FlushAsync();
            }
            finally
            {
                _writeMutex.Dispose();
                _disposed = true;
            }
        }

        #endregion

        #region Private Methods

        private async Task SendInternal(
            byte[]? message,
            bool forcePublishToBus = false,
            CancellationToken cancellationToken = default)
        {
            var writePermissionTaken = false;

#pragma warning disable CS0168 // Variable is declared but never used
            try
            {
                await TakeBufferWriteLock(cancellationToken);
                writePermissionTaken = true;

                // TakeBufferWritePermission can throw OperationCanceledException 
                // exception so we won't get here in that case

                // if nextMessage.Length = 2 * buffer_capacity (or 3 * buffer_capacity)
                // what should we do? we can not chunk message - it must be delivered in 
                // one piece. So, just expand buffer in that case for now
                if (null != message)
                {
                    WriteMessageToBuffer(message);
                }

                if (forcePublishToBus || IsFlushThresholdReached())
                {
                    await PublishToBus();
                }
            }
            catch (Exception ex)
            {
                // Maybe handle ex somehow
                // or throw some custom exception 
                // i.e. ApplicationException<BusMessageWriterErrorType>
                throw;
            }
            finally
            {
                if (writePermissionTaken)
                {
                    ReleaseBufferWriteLock();
                }
            }
#pragma warning restore CS0168 // Variable is declared but never used
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task TakeBufferWriteLock(CancellationToken cancellatioToken)
        {
            await _writeMutex.WaitAsync(cancellatioToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReleaseBufferWriteLock()
        {
            _writeMutex.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsFlushThresholdReached() => _buffer.Length >= _flushBufferThresholdBytes;

        private async Task PublishToBus()
        {
            if (0 == _buffer.Length)
            {
                return;
            }

            await _busConnection.PublishAsync(_buffer.ToArray());
            _buffer.SetLength(0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteMessageToBuffer(byte[] nextMessage)
        {
            _buffer.Write(nextMessage, 0, nextMessage.Length);
        }

        #endregion
    }
}
