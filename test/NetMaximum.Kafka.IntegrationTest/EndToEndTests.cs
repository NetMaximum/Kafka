using System;
using System.IO;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using NetMaximum.Kafka.Producer;
using NetMaximum.UnitTest;
using NetMaximum.XUnit.DockerExtensions;
using Staff.Stream.AvroContracts;
using Xunit;

namespace NetMaximum.Kafka.IntegrationTest;

[Collection("DockerComposeCollection")]
public class EndToEndTests
{
    private static readonly string ComposePath =
        FileUtility.FindFileDirectory(Directory.GetCurrentDirectory(), "NetMaximum.Kafka.IntegrationTest.csproj")!;

    public EndToEndTests(XUnit.DockerExtensions.DockerComposeFixture fixture)
    {
        fixture.Init(Path.Combine(ComposePath, "docker-compose.yml"),
            new WaitFor("rest-proxy", "http://localhost:8082/brokers", TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void Produce_and_consume_an_event()
    {
        // Arrange
        var topic = Guid.NewGuid().ToString();
        var producer =
            new EventProcessorBuilder<IStaffMemberEvent>(new Uri("http://localhost:8081"), topic,
                "host.docker.internal:9092");
        producer.AddSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);

        using var sut = producer.Build();

        var @event = new StaffMemberCreated
        {
            Name = new Name()
            {
                FirstName = "Bob",
                LastName = "Dylan"
            },
            EventId = "eventId",
            TraceId = Guid.NewGuid(),
            StaffMemberId = Guid.NewGuid(),
            TimeFrame = new TimeFrame
            {
                StartDate = DateTime.Now.ToLongDateString(),
                EndDate = DateTime.Now.AddHours(60).ToLongDateString()
            }
        };

        // Act
        sut.Produce(Guid.NewGuid().ToString(), @event);

        var multiTypeConfig = new MultipleTypeConfigBuilder<IStaffMemberEvent>();
        multiTypeConfig.AddType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081"
        };

        var conf = new ConsumerConfig
        {
            GroupId = "MYGROUP",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        var cachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
        var m = new MultipleTypeDeserializer<IStaffMemberEvent>(multiTypeConfig.Build(),
            cachedSchemaRegistryClient);

        using var builder = new ConsumerBuilder<string, IStaffMemberEvent>(conf)
            .SetKeyDeserializer(new AvroDeserializer<string>(cachedSchemaRegistryClient).AsSyncOverAsync())
            .SetValueDeserializer(m.AsSyncOverAsync()).Build();

        builder.Subscribe(topic);

        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var result = builder.Consume(cancellationTokenSource.Token);
        if (result != null)
        {
            throw new AggregateException();
        }
        sut.Dispose();
    }
}