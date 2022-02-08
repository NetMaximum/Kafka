namespace NetMaximum.Kafka.Exceptions;

public class CommitFailedException : NetMaximumKafkaException
{
    public CommitFailedException(string message) : base(message)
    {
    }
}