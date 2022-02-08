using System;
using Confluent.Kafka;
using FluentAssertions;
using Moq;
using NetMaximum.Kafka.Consumer;
using NetMaximum.Kafka.Exceptions;
using NetMaximum.UnitTest;
using Staff.Stream.AvroContracts;
using Xunit;

namespace NetMaximum.Kafka.UnitTest.Consumer;

public class ConsumerResultTests
{
    [Fact]
    public void Result_have_the_correct_key_and_value()
    {
        // Arrange - Act
        var @event = new StaffMemberCreated();
        var sut = new ConsumerResult<string, IStaffMemberEvent>(
            "key", 
            @event, 
            new ConsumeResult<string, IStaffMemberEvent>(), 
            null);
        
        // Assert
        sut.Key.Should().Be("key");
        sut.Value.Should().Be(@event);
    }

    [Fact]
    public void A_result_should_be_committed()
    {
        // Arrange
        var @event = new StaffMemberCreated();
        var consumeResult = new ConsumeResult<string, IStaffMemberEvent>();
        var consumer = new Mock<IConsumer<string, IStaffMemberEvent>>();
        
        var sut = new ConsumerResult<string, IStaffMemberEvent>(
            "key", 
            @event, 
            consumeResult, 
            consumer.Object);
        
        // Act
        sut.Commit();
        
        // Assert
        consumer.Verify(x => x.Commit(consumeResult), Times.Once);
    }
    
    [Fact]
    public void When_commit_fails_a_net_maximum_derived_exception_should_be_thrown()
    {
        // Arrange
        var @event = new StaffMemberCreated();
        var consumeResult = new ConsumeResult<string, IStaffMemberEvent>();
        
        var consumer = new Mock<IConsumer<string, IStaffMemberEvent>>();
        consumer.Setup(x => x.Commit(consumeResult))
            .Throws(new ArgumentException("An exception"));
        
        var sut = new ConsumerResult<string, IStaffMemberEvent>(
            "key", 
            @event, 
            consumeResult, 
            consumer.Object);
        
        // Act
        Action result = () =>
        {
            sut.Commit();
        };

        // Assert
        result.Should().ThrowExactly<CommitFailedException>().WithMessage("An exception");

    }
}