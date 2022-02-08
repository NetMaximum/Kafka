using Avro;
using NetMaximum.Kafka.Exceptions;

namespace NetMaximum.Kafka
{
    /// <summary>
    /// Configuration used to specify the message types supported by <cref>MultipleTypeSerializer</cref> and
    ///  <cref>MultipleTypeDeserializer</cref>. Instances are initialized using <cref>MultipleTypeConfigBuilder</cref>.
    /// </summary>
    public class MultipleTypeConfig
    {
        private readonly MultipleTypeInfo[] _types;
        private readonly bool _ignoreUnknownType;

        internal MultipleTypeConfig(MultipleTypeInfo[] types, bool ignoreUnknownType)
        {
            if (types.Length == 0)
            {
                throw new NoTypesConfiguredException("No types where configured");
            }
            
            _types = types;
            _ignoreUnknownType = ignoreUnknownType;
        }
        
        public IReaderWrapper? CreateReader(Schema writerSchema)
        {
            var type = _types.SingleOrDefault(x => x.Schema.Fullname == writerSchema.Fullname);
            if (type == null)
            {
                if (_ignoreUnknownType)
                {
                    return null;
                }
                
                throw new ArgumentException($"Unexpected type {writerSchema.Fullname}. Supported types need to be added to this {nameof(MultipleTypeConfig)} instance", nameof(writerSchema));
            }

            return type.CreateReader(writerSchema);
        }

        public IEnumerable<MultipleTypeInfo> Types => _types.AsEnumerable();
    }
}
