namespace Orleans.Configuration;

public enum QueueDeclarationMode
{
    /// <summary>
    /// The queue will be declared when the stream provider is initialized.
    /// </summary>
    AtStartup,
    /// <summary>
    /// The queue will not be declared, and it is expected to be created externally.
    /// </summary>
    DoNotDeclare,
    /// <summary>
    /// The queue will be declared on demand, when the first message is sent or received.
    /// </summary>
    OnDemand
}