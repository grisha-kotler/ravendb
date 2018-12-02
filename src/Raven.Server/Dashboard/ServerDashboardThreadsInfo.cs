using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;

namespace Raven.Server.Dashboard
{
    public class ServerDashboardThreadsInfo : NotificationsBase
    {
        public ServerDashboardThreadsInfo(CancellationToken shutdown)
        {
            var options = new ServerDashboardOptions();

            var threadsUsageNotificationSender = new ThreadsInfoNotificationSender(nameof(ServerStore), Watchers, options.ThreadsUsageThrottle, shutdown);
            BackgroundWorkers.Add(threadsUsageNotificationSender);
        }
    }
}
