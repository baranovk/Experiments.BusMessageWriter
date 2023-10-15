using Moq;

namespace Experiments.BusMessageWriter.UnitTests
{
    public class BusMessageWriterTests
    {
        [Test]
        [Explicit]
        [TestCase(100, 128)]
        [TestCase(1000, 128)]
        [TestCase(10000, 128)]
        [TestCase(100000, 128)]
        [TestCase(1000000, 1024)]
        [TestCase(10000000, 128)]
        public async Task BusMessageWriter_Should_Not_LooseMessages(int messagesCount, int flushWriterBufferThresholdBytes)
        {
            var busConnectionMock = new Mock<IBusConnection>();

            // Test code will send to bus messages of random size (from minMessageSizeBytes to maxMessageSizeBytes)
            // Then maximum POSSIBLE bytes sent count will be messagesCount * maxMessageSizeBytes
            int minMessageSizeBytes = 1;
            int maxMessageSizeBytes = default;
            ulong messageBytesSentToBus;
            ulong messageBytesReceivedByBus;

            try
            {
                // Checking if passed parameters are valid for test run
                checked
                {
                    maxMessageSizeBytes = 3 * flushWriterBufferThresholdBytes;
                    messageBytesReceivedByBus = (ulong)maxMessageSizeBytes * (ulong)messagesCount;
                }
            }
            catch (OverflowException)
            {
                Assert.Fail("Invalid test parameters. Try use lower flushWriterBufferThresholdBytes parameter.");
            }

            var random = new Random();
            messageBytesSentToBus = default;
            messageBytesReceivedByBus = default;

            // Mocking IBusConnection with realization
            // that simply counts bytes received by bus
            busConnectionMock
                .Setup(bc => bc.PublishAsync(It.IsAny<byte[]>()))
                .Callback<byte[]>((data) => Interlocked.Add(ref messageBytesReceivedByBus, (ulong)data.Length))
                .Returns(Task.CompletedTask);

            await using (var writer = new SimpleThreadSafeBusMessageWriter(flushWriterBufferThresholdBytes, busConnectionMock.Object))
            {
                // This overload of ForEachAsync uses DegreeOfParallelism == Environment.ProcessorCount
                await Parallel.ForEachAsync(
                    Enumerable.Repeat(default(byte), messagesCount),
                    async (e, ct) =>
                    {
                        var messageLength = random.Next(minMessageSizeBytes, maxMessageSizeBytes + 1);
                        var message = Enumerable.Repeat(default(byte), messageLength).ToArray();
                        await writer.SendMessageAsync(message, ct);
                        // Counting bytes sent to bus
                        Interlocked.Add(ref messageBytesSentToBus, (ulong)message.Length);
                    });
            }

            Assert.That(messageBytesSentToBus, Is.EqualTo(messageBytesReceivedByBus));
        }
    }
}
