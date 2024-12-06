using System.Collections.Concurrent;
using Nito.AsyncEx;

namespace IdempotencySolution
{
    /// <summary>
    /// 异步锁管理
    /// </summary>
    public static class AsyncLockManager
    {
        private static readonly ConcurrentDictionary<string, LockInfo> _locks = new ConcurrentDictionary<string, LockInfo>();
        private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(60); // 锁的过期时间
        private static readonly TimeSpan CleanupInterval = TimeSpan.FromSeconds(30); // 清理间隔时间
        private static readonly Timer CleanupTimer;

        static AsyncLockManager()
        {
            CleanupTimer = new Timer(CleanupExpiredLocks, null, CleanupInterval, CleanupInterval);
        }

        /// <summary>
        /// 获取锁
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static AsyncLock GetLock(string key)
        {
            var lockInfo = _locks.GetOrAdd(key, k => new LockInfo(new AsyncLock(), DateTime.UtcNow));
            lockInfo.LastAccessTime = DateTime.UtcNow;
            return lockInfo.Lock;
        }


        /// <summary>
        /// 清理过期锁
        /// </summary>
        /// <param name="state"></param>
        private static void CleanupExpiredLocks(object? state)
        {
            var now = DateTime.UtcNow;
            foreach (var key in _locks.Keys)
            {
                if (_locks.TryGetValue(key, out var lockInfo))
                {
                    if (now - lockInfo.LastAccessTime > LockTimeout)
                    {
                        _locks.TryRemove(key, out _);
                    }
                }
            }
        }

        /// <summary>
        /// 异步锁信息
        /// </summary>

        private class LockInfo
        {
            public AsyncLock Lock { get; }
            public DateTime LastAccessTime { get; set; }

            public LockInfo(AsyncLock asyncLock, DateTime lastAccessTime)
            {
                Lock = asyncLock;
                LastAccessTime = lastAccessTime;
            }
        }
    }
}
