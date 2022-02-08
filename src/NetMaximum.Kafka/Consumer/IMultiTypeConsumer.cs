namespace NetMaximum.Kafka.Consumer;

public interface IMultiTypeConsumer<T> : IDisposable
{
    ConsumerResult<string, T>? Consume(CancellationToken cancellationToken = default);
}