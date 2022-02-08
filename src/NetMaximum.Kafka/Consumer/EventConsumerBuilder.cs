using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;

namespace NetMaximum.Kafka.Consumer;

public class EventConsumerBuilder<T> : EventProcessorBuilder<T> where T : ISpecificRecord
{

    public bool AutoCommit { get; private set; } = false;
    
    public EventConsumerBuilder(Uri schemaRegistryUrl, string topic, params string[] bootStrapServers) : base(schemaRegistryUrl, topic, bootStrapServers)
    {
    }

    public EventConsumerBuilder<T> WithAutoCommit(bool value)
    {
        AutoCommit = value;
        return this;
    }

    public IMultiTypeConsumer<T> BuildConsumer(string groupId)
    {
        var config =  new ConsumerConfig()
        {
            GroupId = groupId,
            BootstrapServers = string.Join("','", BootStrapServers),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = AutoCommit
        };
        
        var deserializer = new MultipleTypeDeserializer<T>(MultipleTypeConfigBuilder.Build(), CachedSchemaRegistryClient);

        var consumerBuilder = new ConsumerBuilder<string, T>(config)
            .SetKeyDeserializer(new AvroDeserializer<string>(CachedSchemaRegistryClient).AsSyncOverAsync())
            .SetValueDeserializer(deserializer.AsSyncOverAsync()).Build();

        consumerBuilder.Subscribe(Topic);
        
        return new MultiTypeConsumer<T>(consumerBuilder);
    }
    
}