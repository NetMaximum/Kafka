using System;
using Confluent.Kafka;
using FluentAssertions;
using NetMaximum.Kafka;
using NetMaximum.Kafka.Exceptions;
using Xunit;

namespace NetMaximum.UnitTest;

public class MultiTypeConfigTests
{
    [Fact]
    public void At_least_one_type_needs_to_be_configured()
    {
        // Arrange
        var subject = new MultipleTypeConfigBuilder<IEvent>();
        
        // Act 

        var action = new Action(() =>
        {
            subject.Build();
        }).Should().Throw<NoTypesConfiguredException>();
        
    }
}
