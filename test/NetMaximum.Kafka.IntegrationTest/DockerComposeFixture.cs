using Xunit;

namespace NetMaximum.Kafka.IntegrationTest;

[CollectionDefinition("DockerComposeCollection")]
public class DockerComposeFixture : ICollectionFixture<XUnit.DockerExtensions.DockerComposeFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}