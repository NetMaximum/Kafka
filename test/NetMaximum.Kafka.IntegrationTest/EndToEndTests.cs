using System;
using System.IO;
using System.Threading;
using FluentAssertions;
using NetMaximum.Kafka.Consumer;
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
            new WaitFor("rest-proxy", "http://localhost:8082/brokers", TimeSpan.FromSeconds(30)),
            new WaitFor("schema-registry", "http://localhost:8081/subjects", TimeSpan.FromSeconds(30)));
        
    }

    [Fact]
    public void Produce_and_consume_an_event()
    {
        // Arrange
        var topic = Guid.NewGuid().ToString();
        var key = Guid.NewGuid().ToString();
        var processorBuilder = new EventProducerBuilder<IStaffMemberEvent>(new Uri("http://localhost:8081"), topic, "localhost:9092");
        processorBuilder.WithSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        
        var consumerBuilder = new EventConsumerBuilder<IStaffMemberEvent>(new Uri("http://localhost:8081"), topic, "localhost:9092");
        consumerBuilder.WithSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        
        using var sut = processorBuilder.BuildProducer();

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
        
        sut.Produce(key, @event);

        using var consumer = consumerBuilder.BuildConsumer("MyGroup");
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var result = consumer.Consume(cancellationTokenSource.Token);

        // Assert
        result.Should().NotBeNull();
        result.Key.Should().Be(key);
        result.Value.Should().BeEquivalentTo(@event);
    }

    [Fact]
    public void Handles_an_unknown_schema_being_delivered()
    {
        // Arrange
        var topic = Guid.NewGuid().ToString();
        
        var produceEventProcessor = new EventProducerBuilder<IStaffMemberEvent>(new Uri("http://localhost:8081"), topic, "localhost:9092");
        produceEventProcessor.WithSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        produceEventProcessor.WithSerialisationType<StaffMemberTerminated>(StaffMemberTerminated._SCHEMA);
        
        var eventConsumerBuilder = new EventConsumerBuilder<IStaffMemberEvent>(new Uri("http://localhost:8081"), topic, "localhost:9092");
        
        eventConsumerBuilder
            .WithSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA)
            .WithIgnoreUnknownTypes(true);
        
        using var sut = produceEventProcessor.BuildProducer();

        var @invalidEvent = new StaffMemberTerminated
        {
            EventId = "eventId",
            TraceId = Guid.NewGuid(),
            StaffMemberId = Guid.NewGuid(),
            TimeFrame = new TimeFrame
            {
                StartDate = DateTime.Now.ToLongDateString(),
                EndDate = DateTime.Now.AddHours(60).ToLongDateString()
            }
        };
        
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
        
        var key = Guid.NewGuid().ToString();
        sut.Produce(key, invalidEvent);
        sut.Produce(key, @event);

        // Act
        using var sut2 = eventConsumerBuilder.BuildConsumer("my-group");
        
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var result = sut2.Consume(cancellationTokenSource.Token);

        // Assert
        result.Should().NotBeNull();
        result.Key.Should().Be(key);
        result.Value.Should().BeEquivalentTo(@event);
        
    }
}