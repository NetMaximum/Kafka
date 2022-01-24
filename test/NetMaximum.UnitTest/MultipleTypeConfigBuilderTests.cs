using System;
using System.Linq;
using Avro;
using FluentAssertions;
using NetMaximum.Kafka;
using NetMaximum.Kafka.Exceptions;
using Staff.Stream.AvroContracts;
using Xunit;

namespace NetMaximum.UnitTest;

public class MultipleTypeConfigBuilderTests
{
    [Fact]
    public void At_least_one_type_needs_to_be_configured()
    {
        // Arrange
        var subject = new MultipleTypeConfigBuilder<IStaffMemberEvent>();
        
        // Act - Assert 
        new Action(() =>
        {
            subject.Build();
        }).Should().Throw<NoTypesConfiguredException>();
    }

    [Fact]
    public void Types_can_be_configured()
    {
        // Arrange
        var subject = new MultipleTypeConfigBuilder<IStaffMemberEvent>();

        // Act
        subject.AddType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        subject.AddType<StaffMemberTerminated>(StaffMemberTerminated._SCHEMA);
        
        var result = subject.Build();

        // Assert
        result.Types.Count().Should().Be(2);
    }

    [Fact]
    public void The_same_type_can_not_be_added_twice()
    {
        // Arrange
        var subject = new MultipleTypeConfigBuilder<IStaffMemberEvent>();
        subject.AddType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        
        // Act
        var result = new Action(() =>
        {
            subject.AddType<StaffMemberCreated>(StaffMemberCreated._SCHEMA);
        });
        
        // Assert
        result.Should().Throw<ArgumentException>()
            .WithMessage("A type based on schema with the full name \"Staff.Stream.AvroContracts.StaffMemberCreated\" has already been added");
    }

    [Fact]
    public void The_schema_passed_can_not_be_null()
    {
        // Arrange
        var subject = new MultipleTypeConfigBuilder<IStaffMemberEvent>();
        
        // Act
        var result = new Action(()=>
        {
            subject.AddType<StaffMemberCreated>(null);
        });
        
        // Assert
        result.Should().Throw<ArgumentNullException>("Value cannot be null. (Parameter 'readerSchema')");
    }
}

