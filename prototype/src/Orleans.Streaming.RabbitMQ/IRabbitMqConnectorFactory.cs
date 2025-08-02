namespace Orleans.Streaming.RabbitMQ;

internal interface IRabbitMqConnectorFactory
{
    IRabbitMqConnector CreateConnector(string? name);
    IRabbitMqProducerConnector CreateProducerConnector();
    IRabbitMqConsumerConnector CreateConsumerConnector();
}