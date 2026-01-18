using Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");

var rabbitmq = builder.AddRabbitMQ("rabbitmq")
    .WithManagementPlugin()
    .WithImage("rabbitmq", "4.2.2-management");

var orleans = builder.AddOrleans("my-streaming-app")
    .WithClusterId("dev-streaming")
    .WithServiceId("simple-streaming")
    .WithClustering(redis)
    .WithMemoryGrainStorage("Default");

builder.AddProject<Projects.SiloHost>("silohost")
    .WithReference(orleans)
    .WithReference(redis)
    .WithReference(rabbitmq)
    .WaitFor(redis)
    .WaitFor(rabbitmq);

builder.AddProject<Projects.Client>("client")
    .WithReference(orleans.AsClient())
    .WithReference(redis)
    .WithReference(rabbitmq)
    .WaitFor(redis)
    .WaitFor(rabbitmq);

builder.Build().Run();
