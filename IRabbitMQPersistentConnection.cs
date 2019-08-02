using RabbitMQ.Client;
using System;

namespace Platform.EventBus.RabbitMQ
{
    public interface IRabbitMQPersistentConnection
        : IDisposable
    {
        bool IsConnected { get ; }

        bool TryConnect(string hostName = null,
            string userName = null, string password = null);

        IModel CreateModel();
    }
}
