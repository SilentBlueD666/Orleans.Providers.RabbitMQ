using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using RabbitMQ.Client;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Orleans.Streaming.RabbitMQ;

internal interface IRabbitMqGenericConnector : IRabbitMqConnector;
