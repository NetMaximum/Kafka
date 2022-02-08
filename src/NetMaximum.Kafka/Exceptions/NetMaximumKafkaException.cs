namespace NetMaximum.Kafka.Exceptions;

public abstract class NetMaximumKafkaException : Exception
{
    protected NetMaximumKafkaException(string message) : base(message)
    {
    }
}