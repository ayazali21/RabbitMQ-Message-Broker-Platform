using RabbitMQ.Platform;

namespace Platform.EventBus.Abstractions
{
    public interface IEventBus
    {
        void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;
        void SubscribeDynamic<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler;

        void UnsubscribeDynamic<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler;

        void Unsubscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;

        void Publish(IntegrationEvent @event);
    }
}
