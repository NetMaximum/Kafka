using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace NetMaximum.Kafka.Producer;

public class EventProducerBuilder<T> : EventProcessorBuilder<T> where T : ISpecificRecord
{
    public EventProducerBuilder(Uri schemaRegistryUrl, string topic, params string[] bootStrapServers) : base(schemaRegistryUrl, topic, bootStrapServers)
    {
    }
    
    public IMultiTypeProducer<T> BuildProducer()
    {
        
        var config =  new ProducerConfig
        {
            BootstrapServers = string.Join("','", BootStrapServers),
            Acks = Acks.All,
            EnableIdempotence = true,
            CompressionType = CompressionType.Snappy,
            LingerMs = LingerMs,
        };
        
        var serializer = new MultipleTypeSerializer<T>(
            MultipleTypeConfigBuilder.Build(), 
            CachedSchemaRegistryClient,  
            new AvroSerializerConfig
            {
                BufferBytes = 100,
                SubjectNameStrategy = SubjectNameStrategy.Record
            });
        
        return new MultiTypeProducer<T>(new ProducerBuilder<string,T>(config)
            .SetKeySerializer(new AvroSerializer<string>(CachedSchemaRegistryClient).AsSyncOverAsync())
            .SetValueSerializer(serializer.AsSyncOverAsync())
            .Build(), Topic);
    }
}