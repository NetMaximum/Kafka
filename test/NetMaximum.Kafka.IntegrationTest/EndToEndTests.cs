using System;
using System.IO;
using System.Threading;
using FluentAssertions;
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
        var processorBuilder = new EventProcessorBuilder<IStaffMemberEvent>(new Uri("http://localhost:8081"), topic, "host.docker.internal:9092");
        processorBuilder.AddSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        
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
        sut.Produce(Guid.NewGuid().ToString(), @event);

        using var consumer = processorBuilder.BuildConsumer("MyGroup");
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var result = consumer.Consume(cancellationTokenSource.Token);

        // Assert
        result.Should().NotBeNull();

    }
}