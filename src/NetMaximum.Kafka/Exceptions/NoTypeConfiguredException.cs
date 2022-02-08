namespace NetMaximum.Kafka.Exceptions;

public class NoTypesConfiguredException : NetMaximumKafkaException
{
    public NoTypesConfiguredException(string message) : base(message)
    {
    }
}