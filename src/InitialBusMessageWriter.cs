namespace Experiments.BusMessageWriter
{
    public class InitialBusMessageWriter
    {
        #region Fields

        private readonly IBusConnection _connection;
        private readonly MemoryStream _buffer = new();

        #endregion

        #region Constructors

        public InitialBusMessageWriter(IBusConnection connection)
        {
            _connection = connection;
        }

        #endregion

        #region Public Methods

        public async Task SendMessageAsync(byte[] nextMessage)
        {
            _buffer.Write(nextMessage, 0, nextMessage.Length);

            if (_buffer.Length > 1000)
            {
                await _connection.PublishAsync(_buffer.ToArray());
                _buffer.SetLength(0);
            }
        }

        #endregion
    }
}
