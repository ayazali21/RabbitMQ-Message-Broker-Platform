using Autofac;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Platform;
using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Platform.EventBus.Abstractions;
using Platform.EventBus.RabbitMQ.InitializationProperties;

namespace Platform.EventBus.RabbitMQ
{

    public class EventBusRabbitMQ : IEventBus, IDisposable
    {
        private readonly IRabbitMQPersistentConnection _persistentConnection;
        private readonly ILogger<EventBusRabbitMQ> _logger;
        private readonly IEventBusSubscriptionsManager _subsManager;
        private readonly ILifetimeScope _autofac;
        private readonly RabbitMQBasicProperties _basicProperties;
        private readonly string AUTOFAC_SCOPE_NAME = "broker_name_event_bus";
        private IModel _consumerChannel;

        public EventBusRabbitMQ(IRabbitMQPersistentConnection persistentConnection, ILogger<EventBusRabbitMQ> logger,
            ILifetimeScope autofac, IEventBusSubscriptionsManager subsManager, RabbitMQBasicProperties properties)
        {
            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _subsManager = subsManager ?? new InMemoryEventBusSubscriptionsManager();
            _autofac = autofac;
            _basicProperties = (RabbitMQBasicProperties)properties.Clone();

            if (string.IsNullOrWhiteSpace(_basicProperties.broker_name))
                _basicProperties.broker_name = "broker_name_event_bus";

            _consumerChannel = CreateConsumerChannel();

            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }

        private void SubsManager_OnEventRemoved(object sender, string eventName)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            using (var channel = _persistentConnection.CreateModel())
            {
                channel.QueueUnbind(queue: _basicProperties.QueueName,
                    exchange: _basicProperties.broker_name,
                    routingKey: eventName);

                if (_subsManager.IsEmpty)
                {
                    _consumerChannel.Close();
                }
            }
        }

        public void Publish(IntegrationEvent @event)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            var policy = RetryPolicy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(_basicProperties.RetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex.ToString());
                });

            using (var channel = _persistentConnection.CreateModel())
            {
                // @event.EventName - In case of generic event, used in Search API
                var eventName = @event.EventName ?? @event.GetType().Name;

                channel.ExchangeDeclare(exchange: _basicProperties.broker_name,
                                    type: "direct");

                var message = JsonConvert.SerializeObject(@event);
                var body = Encoding.UTF8.GetBytes(message);

                policy.Execute(() =>
                {
                    var props = channel.CreateBasicProperties();
                    props.Expiration = @event.ExpirationTime.TotalMilliseconds.ToString();
                    props.DeliveryMode = 2; // durable. Keep in queue unless consumed
                    channel.BasicPublish(exchange: _basicProperties.broker_name,
                                     routingKey: eventName,
                                     basicProperties: props,
                                     body: body);
                });
            }
        }

        public void SubscribeDynamic<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler
        {
            DoInternalSubscription(eventName);
            _subsManager.AddDynamicSubscription<TH>(eventName);
        }

        public void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();
            DoInternalSubscription(eventName);
            _subsManager.AddSubscription<T, TH>();
        }

        private void DoInternalSubscription(string eventName)
        {
            var containsKey = _subsManager.HasSubscriptionsForEvent(eventName);
            if (!containsKey)
            {
                if (!_persistentConnection.IsConnected)
                {
                    _persistentConnection.TryConnect();
                }

                using (var channel = _persistentConnection.CreateModel())
                {
                    channel.QueueBind(queue: _basicProperties.QueueName,
                                      exchange: _basicProperties.broker_name,
                                      routingKey: eventName);
                }
            }
        }

        public void Unsubscribe<T, TH>()
            where TH : IIntegrationEventHandler<T>
            where T : IntegrationEvent
        {
            _subsManager.RemoveSubscription<T, TH>();
        }

        public void UnsubscribeDynamic<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler
        {
            _subsManager.RemoveDynamicSubscription<TH>(eventName);
        }


        public void Dispose()
        {
            if (_consumerChannel != null)
            {
                _consumerChannel.Dispose();
            }

            _subsManager.Clear();
        }

        private IModel CreateConsumerChannel()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            var channel = _persistentConnection.CreateModel();

            channel.ExchangeDeclare(exchange: _basicProperties.broker_name,
                                 type: "direct");

            channel.QueueDeclare(queue: _basicProperties.QueueName,
                                     durable: _basicProperties.IsDurable,
                                     exclusive: _basicProperties.IsExclusive,
                                     autoDelete: _basicProperties.AutoDelete,
                                     arguments: _basicProperties.ExtendedParameters);


            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                var eventName = ea.RoutingKey;
                var message = Encoding.UTF8.GetString(ea.Body);

                var process = ProcessEvent(eventName, message);

                if(false == _basicProperties.AutomaticAcknowledgement)
                {
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }

                await process;
            };

            channel.BasicConsume(queue: _basicProperties.QueueName,
                                 autoAck: _basicProperties.AutomaticAcknowledgement,
                                 consumer: consumer);


            channel.CallbackException += (sender, ea) =>
            {
                _consumerChannel.Dispose();
                _consumerChannel = CreateConsumerChannel();
            };

            return channel;
        }

        private async Task ProcessEvent(string eventName, string message)
        {
            var retryPolicy = Policy.Handle<Exception>()
            .WaitAndRetry(2,
            retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
            (ex, timeSpan) =>
            {
                Console.WriteLine($"Exception: {ex.Message}");
                Console.WriteLine($"Retrying in {timeSpan.Seconds} seconds");
            })
            .Execute(() => HasSubscription(eventName));

            if (_subsManager.HasSubscriptionsForEvent(eventName))
            {
                using (var scope = _autofac.BeginLifetimeScope(AUTOFAC_SCOPE_NAME))
                {
                    var subscriptions = _subsManager.GetHandlersForEvent(eventName);
                    foreach (var subscription in subscriptions)
                    {
                        if (subscription.IsDynamic)
                        {
                            var handler = scope.ResolveOptional(subscription.HandlerType) as IDynamicIntegrationEventHandler;
                            dynamic eventData = JObject.Parse(message);
                            await handler.Handle(eventData);
                        }
                        else
                        {
                            var eventType = _subsManager.GetEventTypeByName(eventName);
                            var integrationEvent = JsonConvert.DeserializeObject(message, eventType);
                            var handler = scope.ResolveOptional(subscription.HandlerType);
                            var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                            await (Task)concreteType.GetMethod("Handle").Invoke(handler, new object[] { integrationEvent });
                        }
                    }
                }
            }
        }

        private bool HasSubscription(string eventName)
        {
            if (!_subsManager.HasSubscriptionsForEvent(eventName))
                throw new Exception("Subscription Not found");
            return true;
        }
    }
}
