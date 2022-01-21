using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using NetMaximum.Kafka.Consumer;
using Schema = Avro.Schema;

namespace NetMaximum.Kafka.Producer;

public class EventProcessorBuilder<T> where T : ISpecificRecord
{
    private readonly string[] _bootStrapServers;
    private readonly MultipleTypeConfigBuilder<T> _multipleTypeConfigBuilder = new();
    private readonly CachedSchemaRegistryClient _cachedSchemaRegistryClient;
    
    public Uri SchemaRegistryUrl { get; }
    
    public string Topic { get; }

    public string[] BootStrapServers => _bootStrapServers;

    /// <summary>
    /// Amount of time to delay between batches, helps with compression at the cost of latency.
    /// </summary>
    public int LingerMs { get; private set; } = 100;

    public EventProcessorBuilder(Uri schemaRegistryUrl, string topic, params string[] bootStrapServers)
    {
        if (schemaRegistryUrl == null)
        {
            throw new ArgumentNullException(nameof(schemaRegistryUrl));
        }

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Cannot be null or empty", nameof(topic));
        }

        if (bootStrapServers == null || bootStrapServers.Length < 1)
        {
            throw new ArgumentException("Null or Empty", nameof(bootStrapServers));
        }
        
        SchemaRegistryUrl = schemaRegistryUrl;    
        Topic = topic;
        _bootStrapServers = bootStrapServers;
        _cachedSchemaRegistryClient = new CachedSchemaRegistryClient( new SchemaRegistryConfig
        {
            Url = SchemaRegistryUrl.ToString(),
        });
    }

    public IMultiTypeConsumer<T> BuildConsumer(string groupId)
    {
        var config =  new ConsumerConfig()
        {
            GroupId = groupId,
            BootstrapServers = string.Join("','", BootStrapServers),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        var deserializer = new MultipleTypeDeserializer<T>(_multipleTypeConfigBuilder.Build(), _cachedSchemaRegistryClient);

        var consumerBuilder = new ConsumerBuilder<string, T>(config)
            .SetKeyDeserializer(new AvroDeserializer<string>(_cachedSchemaRegistryClient).AsSyncOverAsync())
            .SetValueDeserializer(deserializer.AsSyncOverAsync()).Build();
        
        consumerBuilder.Subscribe(Topic);
        
        return new MultiTypeConsumer<T>(consumerBuilder);
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
        
        //var registry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        var serializer = new MultipleTypeSerializer<T>(
            _multipleTypeConfigBuilder.Build(), 
            _cachedSchemaRegistryClient,  
            new AvroSerializerConfig
            {
                BufferBytes = 100,
                SubjectNameStrategy = SubjectNameStrategy.Record
            });
        
        return new MultiTypeProducer<T>(new ProducerBuilder<string,T>(config)
            .SetKeySerializer(new AvroSerializer<string>(_cachedSchemaRegistryClient).AsSyncOverAsync())
            .SetValueSerializer(serializer.AsSyncOverAsync())
            .Build(), Topic);
    }

    public EventProcessorBuilder<T> AddSerialisationType<TU>(Schema readerSchema) where TU : T
    {
        _multipleTypeConfigBuilder.AddType<TU>(readerSchema);
        return this;
    }

    /// <summary>
    /// /// Amount of time to delay between batches, helps with compression at the cost of latency.
    /// </summary>
    /// <param name="lingerMs">Amount of milliseconds to delay</param>
    /// <returns></returns>
    public EventProcessorBuilder<T> WithLingerMs(int lingerMs)
    {
        this.LingerMs = lingerMs;
        return this;
    }
}