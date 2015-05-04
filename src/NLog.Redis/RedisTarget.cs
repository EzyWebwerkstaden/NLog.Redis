using System;
using System.Collections.Generic;
using NLog.Common;
using NLog.Config;
using NLog.Redis;

namespace NLog.Targets
{
    [Target("Redis")]
    public class RedisTarget : TargetWithLayout
    {
        protected const string ListDataType = "list";
        protected const string ChannelDataType = "channel";

 

        /// <summary>
        /// Sets the host name or IP Address of the redis server
        /// </summary>
        [RequiredParameter]
        public string Host { get; set; }

        /// <summary>
        /// Sets the port number redis is running on
        /// </summary>
        [RequiredParameter]
        public int Port { get; set; }

        /// <summary>
        /// Sets the key to be used for either the list or the pub/sub channel in redis
        /// </summary>
        [RequiredParameter]
        public string Key { get; set; }

        /// <summary>
        /// Sets what redis data type to use, either "list" or "channel"
        /// </summary>
        [RequiredParameter]
        public string DataType { get; set; }

        /// <summary>
        /// Sets the database id to be used in redis if the log entries are sent to a list. Defaults to 0
        /// </summary>
        public int Db { get; set; }
        
        /// <summary>
        /// Sets the password to be used when accessing Redis with authentication required
        /// </summary>
        public string Password { get; set; }

        [ArrayParameter(typeof(Field), "field")]
        public IList<Field> Fields { get; private set; }

        private RedisConnectionManager _redisConnectionManager;

        public RedisTarget()
        {
            Fields = new List<Field>();
        }

        protected override void InitializeTarget()
        {
            base.InitializeTarget();

            _redisConnectionManager = new RedisConnectionManager(Host, Port, Db, Password);
        }
        
        protected override void CloseTarget()
        {
            if (_redisConnectionManager != null)
            {
                _redisConnectionManager.Dispose();    
            }
            
            base.CloseTarget();
        }

        protected override void Write(LogEventInfo logEvent)
        {
            var uncompressedMessage = GetMessage(logEvent);


            var redisDatabase = _redisConnectionManager.GetDatabase();
            switch (DataType.ToLower())
            {
                case ListDataType:
                    redisDatabase.ListRightPush(Key, uncompressedMessage);
                    break;
                case ChannelDataType:
                    redisDatabase.Publish(Key, uncompressedMessage);
                    break;
                default:
                    throw new Exception("no data type defined for redis");
            }
        }

        private string GetMessage(LogEventInfo info)
        {
            return MessageFormatter.GetMessageInner(true, Layout, info, this.Fields);
        }

        protected override void Write(Common.AsyncLogEventInfo logEvent)
        {
            var uncompressedMessage = GetMessage(logEvent.LogEvent);
            var redisDatabase = _redisConnectionManager.GetDatabase();
            switch (DataType.ToLower())
            {
                case ListDataType:
                    redisDatabase.ListRightPushAsync(Key, uncompressedMessage);
                    break;
                case ChannelDataType:
                    redisDatabase.PublishAsync(Key, uncompressedMessage);
                    break;
                default:
                    throw new Exception("no data type defined for redis");
            }
        }

    }
}
