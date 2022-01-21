using Avro.Specific;
using NetMaximum.UnitTest;

namespace NetMaximum.UnitTest
{
    public interface IStaffMemberEvent : ISpecificRecord
    {
    }
}

namespace Staff.Stream.AvroContracts
{
    public partial class StaffMemberCreated : IStaffMemberEvent{}
    public partial class StaffMemberTerminated  : IStaffMemberEvent{}
}
