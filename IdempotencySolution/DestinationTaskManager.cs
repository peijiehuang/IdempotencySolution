using System.Collections.Concurrent;

namespace IdempotencySolution
{
    /// <summary>
    /// 目的地任务管理
    /// </summary>
    public static class DestinationTaskManager
    {
        private static readonly ConcurrentDictionary<string, int> _destinationTasks = new ConcurrentDictionary<string, int>();

        /// <summary>
        /// 获取任务数
        /// </summary>
        /// <param name="taskName"></param>
        /// <param name="destination"></param>
        /// <returns></returns>
        public static int TryGetDestinationTask(string taskName, out string destination)
        {
            destination = $"DestinationTask_{taskName}";
            return _destinationTasks.AddOrUpdate(destination, 0, (key, count) => ++count);

        }

        /// <summary>
        /// 移除key
        /// </summary>
        /// <param name="destination"></param>
        public static void RemoveTask(string destination)
        {
            _destinationTasks.TryRemove(destination, out _);
        }
    }
}
