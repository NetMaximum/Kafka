using Confluent.Kafka;

namespace NetMaximum.Kafka.Producer;

public class MultiTypeProducer<T> : IMultiTypeProducer<T>
{
    private readonly IProducer<string, T> _producer;
    private readonly string _defaultTopic;

    public MultiTypeProducer(IProducer<string, T> producer, string defaultTopic)
    {
        _producer = producer;
        _defaultTopic = defaultTopic;
    }

    public void Produce(string key, T value)
    {
        _producer.Produce(_defaultTopic, new Message<string, T> {Key = key, Value = value});
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}