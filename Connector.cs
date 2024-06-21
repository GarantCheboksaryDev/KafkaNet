using System;
using System.IO;
using System.Net;
using System.Text;
using System.Xml;
using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace KafkaNet
{
    public class Messages
    {
        public string MessageId;
        public DateTime Created;
        public string Value;
        public string Key;

        public Messages()
        {
        }

        public Messages(string messageId, DateTime created, string value)
        {
            this.MessageId = messageId;
            this.Created = created;
            this.Value = value;
        }
    }

    public class Response
    {
        public List<Messages> Messages;
        public string Error;

        public Response()
        {
        }

        public Response(List<Messages> messages, string error)
        {
            this.Messages = messages;
            this.Error = error;
        }
    }

    public class Connector
    {
        public string BootstrapServers;
        public string UserName;
        public string Password;
        public string ConsumerGroupId;

        public Connector(string bootstrapServers, string userName, string password, string consumerGroupId)
        {
            this.BootstrapServers = bootstrapServers;
            this.UserName = userName;
            this.Password = password;
            this.ConsumerGroupId = consumerGroupId;
        }

        /// <summary>
        /// Получить сообщения из топика.
        /// </summary>
        /// <param name="connectSettings">Настройки подключения.</param>
        /// <param name="topic">Наименование топика.</param>
        /// <returns>Структурированный ответ.</returns>
        public Response ReadMessagesFromTopic(Connector connectSettings, string topic)
        {
            var response = new Response();

            var config = CreateConsumerConfig(connectSettings);

            List<Messages> messagesResponse = new List<Messages>();

            if (!Library.IsLoaded)
            {
                var pathToLibrd = string.Empty;
                if (GetOperatingSystem() == OSPlatform.Windows)
                {
                    pathToLibrd = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location),
                        $"librdkafka\\{(Environment.Is64BitOperatingSystem ? "x64" : "x86")}\\librdkafka.dll");
                    Library.Load(pathToLibrd);
                }
            }

            using (var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) =>
                {
                    return;
                }
                )
                .Build())
            {
                
                consumer.Subscribe(topic);
                
                try
                {
                    while (true)
                    {
                        double interval = config.MaxPollIntervalMs.Value - 1000;

                        var cr = consumer.Consume(TimeSpan.FromMilliseconds(interval));

                        if (cr != null)
                        {
                            try
                            {
                                Messages messageResponse = new Messages();
                                messageResponse.Created = cr.Message.Timestamp.UtcDateTime;
                                messageResponse.MessageId = cr.Offset.Value.ToString();
                                messageResponse.Value = cr.Message.Value;
                                messageResponse.Key = cr.Message.Key != null ? cr.Message.Key.ToString() : string.Empty;

                                consumer.StoreOffset(cr);

                                messagesResponse.Add(messageResponse);
                            }
                            catch (Exception)
                            {

                            }

                        }
                        else
                        {
                            consumer.Close();
                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    var error = "Во время обработки сообщений произошла ошибка " + ex;
                    response.Error = error;
                    response.Messages = messagesResponse;

                    return response;
                }
            }

            response.Error = string.Empty;
            response.Messages = messagesResponse;

            return response;
        }

        /// <summary>
        /// Проверить подключение к сервису.
        /// </summary>
        /// <param name="connectSettings">Настройки подключения.</param>
        /// <param name="topic">Наименование топика.</param>
        /// <returns>True - если подключение успешно установлено, иначе false.</returns>
        public bool CheckConnection(Connector connectSettings, string topic)
        {
            var connect = true;

            var config = CreateConsumerConfig(connectSettings);

            if (!Library.IsLoaded)
            {
                var pathToLibrd = string.Empty;
                if (GetOperatingSystem() == OSPlatform.Windows)
                {
                    pathToLibrd = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location),
                        $"librdkafka\\{(Environment.Is64BitOperatingSystem ? "x64" : "x86")}\\librdkafka.dll");
                    Library.Load(pathToLibrd);
                }
            }

            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) =>
                {

                    connect = false;
                }
                )
                .Build())
            {
                consumer.Subscribe(topic);

                double interval = config.MaxPollIntervalMs.Value - 1000;

                consumer.Consume(TimeSpan.FromMilliseconds(interval));
            }

            return connect;
        }

        /// <summary>
        /// Создать конфиг Consumer.
        /// </summary>
        /// <param name="connectSettings">Параметры подключения.</param>
        /// <returns>Конфиг Consumer.</returns>
        public ConsumerConfig CreateConsumerConfig(Connector connectSettings)
        {
            return new ConsumerConfig
            {
                GroupId = connectSettings.ConsumerGroupId,
                BootstrapServers = connectSettings.BootstrapServers,
                FetchMaxBytes = 1800000000,
                MessageMaxBytes = 1000000000,
                ReceiveMessageMaxBytes = 1850000000,
                SecurityProtocol = SecurityProtocol.Plaintext,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                MaxPollIntervalMs = 10000,
                SessionTimeoutMs = 10000,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false
            };
        }

        public static OSPlatform GetOperatingSystem()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return OSPlatform.OSX;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return OSPlatform.Linux;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return OSPlatform.Windows;
            }

            throw new Exception("Cannot determine operating system!");
        }
    }

    public class KafkaProducer
    {
        public string BootstrapServers;
        public string UserName;
        public string Password;
        public string LogPath;

        public KafkaProducer(string bootstrapServers, string userName, string password, string logPath)
        {
            this.BootstrapServers = bootstrapServers;
            this.UserName = userName;
            this.Password = password;
            this.LogPath = logPath;
        }

        public string ProduceMessage(KafkaProducer connectSettings, string topicName, string jsonValueMessage, string keyMessage)
        {

            var config = new ProducerConfig
            {
                BootstrapServers = connectSettings.BootstrapServers,
                MessageMaxBytes = 1000000000,
                ReceiveMessageMaxBytes = 1850000000,
                SecurityProtocol = SecurityProtocol.Plaintext
            };
            if (!Library.IsLoaded)
            {
                var pathToLibrd = string.Empty;
                if (Connector.GetOperatingSystem() == OSPlatform.Windows)
                {
                    pathToLibrd = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location),
                        $"librdkafka\\{(Environment.Is64BitOperatingSystem ? "x64" : "x86")}\\librdkafka.dll");
                    Library.Load(pathToLibrd);
                }
            }
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                try
                {
                    var dr = p.ProduceAsync(topicName, new Message<string, string> { Key = keyMessage, Value = jsonValueMessage });
                }
                catch (ProduceException<Null, string> e)
                {
                    return e.Error.Reason;
                }
            }

            return string.Empty;
        } 
    }
}
