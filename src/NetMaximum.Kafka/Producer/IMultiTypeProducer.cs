namespace NetMaximum.Kafka.Producer;

public interface IMultiTypeProducer<in T> : IDisposable
{
    void Produce(string key, T value);
}