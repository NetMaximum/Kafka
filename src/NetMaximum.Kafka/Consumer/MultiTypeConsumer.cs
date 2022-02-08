using Confluent.Kafka;

namespace NetMaximum.Kafka.Consumer;

public class MultiTypeConsumer<T> : IMultiTypeConsumer<T>
{
    private readonly IConsumer<string, T> _innerConsumer;

    internal MultiTypeConsumer(IConsumer<string, T> innerConsumer)
    {
        
        _innerConsumer = innerConsumer;
    }

    public ConsumerResult<string, T>? Consume(CancellationToken cancellationToken = default)
    {
        bool foundResult = false;

        /* Loops over the consume result until it's not null
         it does this to deal with unknown events being on a stream, which is valid.
        */
        while (!foundResult)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            var result = _innerConsumer.Consume(cancellationToken);

            if (result.Message != null && result.Message.Value != null)
            {
                return new(result.Message.Key, result.Message.Value, result, _innerConsumer);
            }
        }

        return null;
    }

    public void Dispose()
    {
        _innerConsumer.Dispose();
    }
}