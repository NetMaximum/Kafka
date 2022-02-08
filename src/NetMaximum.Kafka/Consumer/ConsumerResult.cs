using Confluent.Kafka;
using NetMaximum.Kafka.Exceptions;

namespace NetMaximum.Kafka.Consumer;

public class ConsumerResult<TKey, TValue>
{
    /// <summary>
    /// The original result used to commit the offset.
    /// </summary>
    private readonly ConsumeResult<string, TValue> _consumeResult;

    /// <summary>
    /// The underlying stream consumer
    /// </summary>
    private readonly IConsumer<string, TValue> _innerConsumer;

    public TKey Key { get; }
    public TValue Value { get; }

    /// <summary>
    ///  Commits the stream offset, only required if autocommit is disabled
    /// </summary>
    public void Commit()
    {
        try
        {
            _innerConsumer.Commit(_consumeResult);
        }
        catch (Exception e)
        {
            throw new CommitFailedException(e.Message);
        }
    }

    internal ConsumerResult(TKey key, TValue value, ConsumeResult<string, TValue> consumeResult,
        IConsumer<string, TValue> innerConsumer)
    {
        _consumeResult = consumeResult;
        _innerConsumer = innerConsumer;
        Key = key;
        Value = value;
    }
}