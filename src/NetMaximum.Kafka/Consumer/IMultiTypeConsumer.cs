namespace NetMaximum.Kafka.Consumer;

public interface IMultiTypeConsumer<in T> : IDisposable
{
    object Consume(CancellationToken cancellationToken = default);
}