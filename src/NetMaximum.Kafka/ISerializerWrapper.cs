using Confluent.Kafka;

namespace NetMaximum.Kafka
{
    /// <summary>
    /// Wraps generic AvroSerializer
    /// </summary>
    public interface ISerializerWrapper
    {
        Task<byte[]> SerializeAsync(object data, SerializationContext context);
    }
}
