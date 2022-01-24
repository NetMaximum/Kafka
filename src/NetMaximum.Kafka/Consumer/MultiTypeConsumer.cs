using Confluent.Kafka;

namespace NetMaximum.Kafka.Consumer;

public class MultiTypeConsumer<T> : IMultiTypeConsumer<T>
{
    private readonly IConsumer<string, T> _innerConsumer;

    internal MultiTypeConsumer(IConsumer<string,T> innerConsumer)
    {
        _innerConsumer = innerConsumer;
    }
    
    public object Consume(CancellationToken cancellationToken = default)
    {
        return _innerConsumer.Consume();
    }

    public void Dispose()
    {
        _innerConsumer.Dispose();
    }
}