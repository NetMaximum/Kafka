using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace NetMaximum.Kafka.Producer;

public abstract class ProducerWrapper<T> where T : ISpecificRecord
{
    private readonly string _topic;
    private readonly string _bootStrapServers;
    private readonly string _schemaReg;

    private readonly object _lock = new();
    private readonly Lazy<IMultiTypeProducer<T>> _producer;

    protected ProducerWrapper(string topic, string bootStrapServers, string schemaReg)
    {
        _topic = topic;
        _bootStrapServers = bootStrapServers;
        _schemaReg = schemaReg;
        _producer = new Lazy<IMultiTypeProducer<T>>(Factory);
    }

    protected abstract void RegisterTypes(MultipleTypeConfigBuilder<T> builder);

    public IMultiTypeProducer<T> Build()
    {
        // Prevents threading issues.
        lock (_lock)
        {
            return _producer.Value;    
        }
    }

    private IMultiTypeProducer<T> Factory()
    {
        var multipleTypeConfig = new MultipleTypeConfigBuilder<T>();
        RegisterTypes(multipleTypeConfig);
        
        var config =  new ProducerConfig
        {
            BootstrapServers = _bootStrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            CompressionType = CompressionType.Snappy,
            LingerMs = 100,
    
        };
        
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = _schemaReg,
        };

        
        var registry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        var serializer = new MultipleTypeSerializer<T>(
            multipleTypeConfig.Build(), 
            registry,  
            new AvroSerializerConfig
            {
                BufferBytes = 100,
                SubjectNameStrategy = SubjectNameStrategy.Record
            });

        return new MultiTypeProducer<T>(new ProducerBuilder<string, T>(config)
            .SetKeySerializer(new AvroSerializer<string>(registry).AsSyncOverAsync())
            .SetValueSerializer(serializer.AsSyncOverAsync())
            .Build(), _topic);
    }
}
