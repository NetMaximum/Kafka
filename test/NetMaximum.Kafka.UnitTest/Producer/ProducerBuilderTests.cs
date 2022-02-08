using System;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FluentAssertions;
using NetMaximum.Kafka.Consumer;
using NetMaximum.Kafka.Producer;
using NetMaximum.UnitTest;
using Staff.Stream.AvroContracts;
using Xunit;

namespace NetMaximum.Kafka.UnitTest.Producer;

public class ProducerBuilderTests
{
    private readonly EventProducerBuilder<IStaffMemberEvent> _sut = new( new Uri("http://localhost:6000"), "topic", "http://localhost:9001");
    private readonly EventConsumerBuilder<IStaffMemberEvent> _cut = new( new Uri("http://localhost:6000"), "topic", "http://localhost:9001");

    [Fact]
    public void Can_build_a_new_producer()
    {
        // Arrange
        _sut.WithSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        
        // Act
        var result = _sut.BuildProducer();
        
        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Can_build_a_new_consumer()
    {
        // Arrange
        _cut.WithSerialisationType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        
        // Act
        var result = _cut.BuildConsumer("my-group");
        
        // Assert
        result.Should().NotBeNull();
    }
    
    [Fact]
    public void Must_have_a_valid_registry_uri_format()
    {
        // Arrange - Act
        var subject = new Action(() =>
        {
            var _ = new EventProducerBuilder<IStaffMemberEvent>(null, "topic", "http://localhost:9001");
        });
        
        // Assert
        subject.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null. (Parameter 'schemaRegistryUrl')");
    }

    [Theory]
    [InlineData(" ")]
    [InlineData("")]
    [InlineData(null)]
    public void Must_have_a_topic(string value)
    {
        // Arrange - Act
        var subject = new Action(() =>
        {
            var _ = new EventProducerBuilder<IStaffMemberEvent>(new Uri("http://local"), value, "http://localhost:9001");
        });
        
        // Assert
        subject.Should().Throw<ArgumentException>().WithMessage("Cannot be null or empty (Parameter 'topic')");
    }

    [Fact]
    public void Must_not_pass_null_for_bootstrap_server()
    {
        // Arrange - Act
        var subject = new Action(() =>
        {
            var _ = new EventProducerBuilder<IStaffMemberEvent>(new Uri("http://local"), "topic", null);
        });
        
        // Assert
        subject.Should().Throw<ArgumentException>().WithMessage("Null or Empty (Parameter 'bootStrapServers')");
    }
    
    [Fact]
    public void Must_have_valid_bootstrap_server()
    {
        // Arrange - Act
        var subject = new Action(() =>
        {
            var _ = new EventProducerBuilder<IStaffMemberEvent>(new Uri("http://local"), "topic", Array.Empty<string>());
        });
        
        // Assert
        subject.Should().Throw<ArgumentException>().WithMessage("Null or Empty (Parameter 'bootStrapServers')");
    }
    
    [Theory]
    [InlineData(1500)]
    [InlineData(3000)]
    public void Can_set_the_schema_registry_url(int lingerMs)
    {
        // Arrange
        // Act
        _sut.WithLingerMs(lingerMs);
        
        // Assert
        _sut.LingerMs.Should().Be(lingerMs);

    }
}