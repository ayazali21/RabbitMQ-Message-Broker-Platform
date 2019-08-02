using System;
using System.Collections.Generic;
using System.Text;

namespace Platform.EventBus.RabbitMQ.InitializationProperties
{
    public class RabbitMQBasicProperties: ICloneable
    {
        public string broker_name { get; set; }
        public string QueueName { get; set; }
        public int RetryCount { get; set; } = 5;
        public bool IsDurable { get; set; } = false;
        public bool IsExclusive { get; set; } = false;
        public bool AutoDelete { get; set; } = false;
        public bool AutomaticAcknowledgement { get; set; } = false;
        public IDictionary<string,object> ExtendedParameters { get; set; }

        public object Clone()
        {
            return this.MemberwiseClone();
        }
    }
}
