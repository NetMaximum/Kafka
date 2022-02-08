namespace NetMaximum.Kafka.Consumer;

public class ConsumerResult<TKey, TValue>
{
    public TKey Key { get; }
    public TValue Value { get; }
    
    public ConsumerResult(TKey key, TValue value)
    {
        Key = key;
        Value = value;
    }
}