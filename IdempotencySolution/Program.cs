namespace IdempotencySolution
{
    using Azure.Core;
    using SqlSugar;
    using System;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            // 初始化数据库
            var connectionString = "Server=localhost;Database=testDB;Uid=root;Pwd=123456;";

            // 创建db客户端对象
            var db = CreateDbClient(connectionString);

            // 创建数据库
            db.DbMaintenance.CreateDatabase();

            // 确保创建表
            db.CodeFirst.InitTables<ScheduleTask>();

            // 模拟并发插入任务
            Parallel.For(0, 10, i =>
            {
                // 每个线程都创建一个新的 db 客户端实例，确保独立连接
                var threadDb = CreateDbClient(connectionString);
                var taskName = "任务名称";

                //模拟接口被多次重复调用，导致请求成功后发生了幂等性问题
                //InsertScheduleTaskWithTransaction(threadDb, taskName);

                //带异步锁解决幂等性问题
                InsertScheduleTaskWithTransactionLock(threadDb, taskName);
            });

            Console.WriteLine("最终数据库中的任务：");
            var result = db.Queryable<ScheduleTask>().ToListAsync().GetAwaiter().GetResult();
            foreach (var item in result)
            {
                Console.WriteLine($"ID: {item.Id}, Name: {item.Name}, CreatedAt: {item.CreatedAt}");
            }
        }

        /// <summary>
        /// 创建db客户端对象
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        static SqlSugarClient CreateDbClient(string connectionString)
        {
            // 每次都创建新的 SqlSugarClient 实例，确保每个请求都是独立的连接
            return new SqlSugarClient(new ConnectionConfig()
            {
                ConnectionString = connectionString,
                DbType = DbType.MySql,
                IsAutoCloseConnection = true
            });
        }

        /// <summary>
        /// 创建任务且有事务
        /// </summary>
        /// <param name="db"></param>
        /// <param name="taskName"></param>
        static void InsertScheduleTaskWithTransaction(SqlSugarClient db, string taskName)
        {
            // 使用事务保证线程内的操作原子性
            db.Ado.BeginTran();

            try
            {
                // 查询是否存在任务号
                var existingTask = db.Queryable<ScheduleTask>()
                    .Any(x => x.Name == taskName);

                //不存在
                if (!existingTask)
                {
                    // 插入任务
                    db.Insertable(new ScheduleTask()
                    {
                        Name = taskName,
                        CreatedAt = DateTime.Now
                    }).ExecuteCommand();
                    Console.WriteLine($"插入成功任务名称: {taskName}");
                }
                else
                {
                    Console.WriteLine($"任务名称： {taskName} 已经存在");
                }

                // 提交事务
                db.Ado.CommitTran();
            }
            catch (Exception ex)
            {
                // 发生异常时回滚事务
                db.Ado.RollbackTran();
                Console.WriteLine($"插入失败任务名称: {taskName}, {ex.Message}");
            }
        }


        /// <summary>
        /// 创建任务且有事务（带锁）
        /// </summary>
        /// <param name="db"></param>
        /// <param name="taskName"></param>
        static async void InsertScheduleTaskWithTransactionLock(SqlSugarClient db, string taskName)
        {
            var asyncLock = AsyncLockManager.GetLock(taskName);

            using (await asyncLock.LockAsync())
            {
                // 使用事务保证线程内的操作原子性
                db.Ado.BeginTran();

                try
                {
                    // 查询是否存在任务号
                    var existingTask = db.Queryable<ScheduleTask>()
                        .Any(x => x.Name == taskName);

                    //不存在
                    if (!existingTask)
                    {
                        // 插入任务
                        db.Insertable(new ScheduleTask()
                        {
                            Name = taskName,
                            CreatedAt = DateTime.Now
                        }).ExecuteCommand();
                        Console.WriteLine($"插入成功任务名称: {taskName}");
                    }
                    else
                    {
                        Console.WriteLine($"任务名称： {taskName} 已经存在");
                    }

                    // 提交事务
                    db.Ado.CommitTran();
                }
                catch (Exception ex)
                {
                    // 发生异常时回滚事务
                    db.Ado.RollbackTran();
                    Console.WriteLine($"插入失败任务名称: {taskName}, {ex.Message}");
                }
            }

        }
    }

    /// <summary>
    /// 调度任务表
    /// </summary>
    public class ScheduleTask
    {
        /// <summary>
        /// 主键ID
        /// </summary>
        [SugarColumn(IsPrimaryKey = true, IsIdentity = true)]
        public int Id { get; set; }

        /// <summary>
        /// 任务名称
        /// </summary>
        [SugarColumn(Length = 100, IsNullable = false)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// 创建时间
        /// </summary>
        public DateTime CreatedAt { get; set; }
    }
}
