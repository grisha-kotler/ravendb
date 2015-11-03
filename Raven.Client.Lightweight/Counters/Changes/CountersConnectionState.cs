using System;
using System.Threading.Tasks;
using Raven.Abstractions.Counters.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.Counters.Changes
{
    public class CountersConnectionState : ConnectionStateBase
    {
        private readonly Func<CountersConnectionState, Task> ensureConnection;

        public CountersConnectionState(Action onZero, Func<CountersConnectionState, Task> ensureConnection, Task task)
            : base(onZero, task)
        {
            this.ensureConnection = ensureConnection;
        }

        protected override Task EnsureConnection()
        {
            return ensureConnection(this);
        }

        public event Action<ChangeNotification> OnChangeNotification = (x) => { };
        public void Send(ChangeNotification changeNotification)
        {
            var onCounterChangeNotification = OnChangeNotification;
            onCounterChangeNotification?.Invoke(changeNotification);
        }

        public event Action<StartingWithNotification> OnCountersStartingWithNotification = (x) => { };
        public void Send(StartingWithNotification changeNotification)
        {
            var onCounterChangeNotification = OnCountersStartingWithNotification;
            onCounterChangeNotification?.Invoke(changeNotification);
        }

        public event Action<InGroupNotification> OnCountersInGroupNotification = (x) => { };
        public void Send(InGroupNotification changeNotification)
        {
            var onCounterChangeNotification = OnCountersInGroupNotification;
            onCounterChangeNotification?.Invoke(changeNotification);
        }

        public event Action<BulkOperationNotification> OnBulkOperationNotification = (x) => { };
        public void Send(BulkOperationNotification bulkOperationNotification)
        {
            var onBulkOperationNotification = OnBulkOperationNotification;
            onBulkOperationNotification?.Invoke(bulkOperationNotification);
        }
    }
}
