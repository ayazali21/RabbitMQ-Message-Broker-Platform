using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ.Platform
{
    public class IntegrationEvent
    {
        public IntegrationEvent()
        {
            Id = Guid.NewGuid();
            CreationDate = DateTime.UtcNow;
        }

        public Guid Id { get; }
        public string EventName { get; set; }
        public DateTime CreationDate { get; }
        public TimeSpan ExpirationTime { get; set; } = TimeSpan.FromMinutes(1);
    }
}
