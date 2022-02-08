using Avro.Specific;
using Confluent.SchemaRegistry;
using Schema = Avro.Schema;

namespace NetMaximum.Kafka;

public abstract class EventProcessorBuilder<T> where T : ISpecificRecord
{
    private readonly string[] _bootStrapServers;
    protected readonly MultipleTypeConfigBuilder<T> MultipleTypeConfigBuilder = new();
    protected readonly CachedSchemaRegistryClient CachedSchemaRegistryClient;
    
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
        CachedSchemaRegistryClient = new CachedSchemaRegistryClient( new SchemaRegistryConfig
        {
            Url = SchemaRegistryUrl.ToString(),
        });
    }

    /// <summary>
    /// Make a schema available for serialization and deserialization.
    /// </summary>
    /// <param name="readerSchema"></param>
    /// <typeparam name="TU"></typeparam>
    /// <returns>Builder</returns>
    public EventProcessorBuilder<T> WithSerialisationType<TU>(Schema readerSchema) where TU : T
    {
        MultipleTypeConfigBuilder.AddType<TU>(readerSchema);
        return this;
    }

    /// <summary>
    /// Indicate during deserialization if unknown types in the event stream should be ignored.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public EventProcessorBuilder<T> WithIgnoreUnknownTypes(bool value)
    {
        MultipleTypeConfigBuilder.WithIgnoreUnknownTypes(value);
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