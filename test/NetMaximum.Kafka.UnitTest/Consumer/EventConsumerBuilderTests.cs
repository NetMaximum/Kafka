using System;
using FluentAssertions;
using NetMaximum.Kafka.Consumer;
using NetMaximum.UnitTest;
using Xunit;

namespace NetMaximum.Kafka.UnitTest.Consumer;

public class EventConsumerBuilderTests
{
    private EventConsumerBuilder<IStaffMemberEvent> _sut = new(new Uri("http://localhost:8081"), "Topic", "localhost:9092");

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Sets_auto_commit_correctly(bool value)
    {
        // Arrange // Act
        _sut.WithAutoCommit(value);
        
        // Assert
        _sut.AutoCommit.Should().Be(value);
    } 
}