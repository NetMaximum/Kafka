using Avro.IO;

namespace NetMaximum.Kafka
{
    /// <summary>
    /// Wraps generic Avro SpecificReader
    /// </summary>
    public interface IReaderWrapper
    {
        object Read(BinaryDecoder decoder);
    }
}
