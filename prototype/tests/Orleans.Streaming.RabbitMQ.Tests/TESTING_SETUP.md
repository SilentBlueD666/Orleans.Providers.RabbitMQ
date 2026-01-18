# RabbitMQ Streaming Tests Setup

## Overview

This test project uses **Testcontainers** to automatically manage a RabbitMQ container for integration testing. The setup ensures that:

1. A fresh RabbitMQ container is started before each test run
2. The Orleans test cluster connects to the running container
3. The container is properly cleaned up after tests complete

## Architecture

### Key Components

#### `RabbitMqTestClusterFixture`
- **Purpose**: xUnit fixture that manages the complete test lifecycle
- **Responsibilities**:
  - Starts a RabbitMQ container using Testcontainers
  - Constructs the connection string from the running container
  - Initializes the Orleans test cluster
  - Cleans up resources on disposal

#### `BaseTestClusterFixture`
- **Purpose**: Base class for all Orleans cluster test fixtures
- **Features**:
  - Manages Orleans `TestCluster` initialization and disposal
  - Provides access to `GrainFactory`, `Client`, and logging services
  - Allows derived classes to customize cluster configuration via `ConfigureTestCluster()`

#### Test Grains
- **`IProducerGrain` / `ProducerGrain`**: Sends messages to RabbitMQ streams
- **`IConsumerGrain` / `ConsumerGrain`**: Subscribes to and receives messages from streams

## Running Tests

### Prerequisites
- Docker must be running (Testcontainers manages container lifecycle)
- .NET 8+ SDK

### Test Execution

```bash
# Run all RabbitMQ streaming tests
dotnet test tests/Orleans.Streaming.RabbitMQ.Tests/Orleans.Streaming.RabbitMQ.Tests.csproj

# Run specific test
dotnet test tests/Orleans.Streaming.RabbitMQ.Tests/Orleans.Streaming.RabbitMQ.Tests.csproj -k "ProducerSendsAndConsumerReceives"

# Run with verbose output
dotnet test tests/Orleans.Streaming.RabbitMQ.Tests/Orleans.Streaming.RabbitMQ.Tests.csproj --verbosity detailed
```

## Container Configuration

The RabbitMQ container is configured with:
- **Image**: `rabbitmq:3.13-management`
- **Default Credentials**: `guest:guest`
- **Port Mapping**: Container port 5672 (AMQP) → Random host port
- **Labels**: `test=orleans-rabbitmq`

The fixture automatically resolves the mapped port and constructs the connection string dynamically.

## Orleans Cluster Configuration

The cluster is configured with:
- **Stream Provider**: `rabbit-mq-stream-provider`
- **Exchange Name**: `orleans-test-exchange`
- **Queue Prefix**: `orleans-test-queue-`
- **Connection String**: Dynamically set from the running container

## Extending Tests

To add new tests:

1. Create a test class that uses `RabbitMqTestClusterFixture`:
   ```csharp
   public class MyStreamingTest : IClassFixture<RabbitMqTestClusterFixture>
   {
       private readonly RabbitMqTestClusterFixture _fixture;
       
       public MyStreamingTest(RabbitMqTestClusterFixture fixture)
       {
           _fixture = fixture;
       }
       
       [Fact]
       public async Task MyTest()
       {
           var grain = _fixture.GrainFactory.GetGrain<IMyGrain>(id);
           // ... test logic
       }
   }
   ```

2. Use the fixture's `GrainFactory` and `Client` to interact with grains and the cluster

## Troubleshooting

### Container fails to start
- Ensure Docker is running
- Check that port 5672 is not already in use
- Verify the RabbitMQ image can be pulled: `docker pull rabbitmq:4.2.2-management`

### Connection timeouts
- The container needs a few seconds to become ready
- Tests implement polling with timeouts to handle this
- Increase timeout values if tests fail on slow systems

### Messages not received
- Check that both producer and consumer use the same stream GUID
- Verify the message namespace matches: `MessageStreamNamespace`
- Ensure RabbitMQ exchange and queues are properly created

## References

- [Testcontainers for .NET](https://testcontainers.com/modules/rabbitmq/)
- [Orleans Streaming Documentation](https://docs.microsoft.com/en-us/dotnet/orleans/streaming)
- [RabbitMQ AMQP Specification](https://github.com/rabbitmq/amqp-0.9.1-spec)
