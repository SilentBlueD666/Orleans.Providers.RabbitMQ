namespace Orleans.Streaming.RabbitMQ;

internal interface IRabbitMqConnectorFactory
{
    IRabbitMqConnector CreateConnector(string? name);
    IRabbitMqConnector CreateProducerConnector();
    IRabbitMqConnector CreateConsumerConnector();
}