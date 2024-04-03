﻿using System;
using System.IO;
using System.Net;
using System.Text;
using System.Xml;
using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace KaffkaNet
{
    public class Response
    {
        public string Topic;
        public List<ResponseMessages> Messages;

        public Response()
        {
        }

        public Response(string topic, List<ResponseMessages> messages)
        {
            Topic = topic;
            Messages = messages;
        }
    }

    public class ResponseMessages
    {
        public string MessageId;
        public DateTime Created;
        public string Value;

        public ResponseMessages()
        {
        }

        public ResponseMessages(string messageId, DateTime created, string value)
        {
            MessageId = messageId;
            Created = created;
            Value = value;
        }
    }

    public class Connector
    {
        private readonly string BootstrapServers;
        private readonly string UserName;
        private readonly string Password;
        private readonly string ConsumerGroupId;

        public Connector(string bootstrapServers, string userName, string password, string consumerGroupId)
        {
            BootstrapServers = bootstrapServers;
            UserName = userName;
            Password = password;
            ConsumerGroupId = consumerGroupId;
        }

        /// <summary>
        /// Получить сообщения из топика.
        /// </summary>
        /// <param name="connectSettings">Настройки подключения.</param>
        /// <param name="topic">Наименование топика.</param>
        /// <param name="logpath">Путь к логфайлам.</param>
        /// <returns>Структурированный ответ.</returns>
        public Response ReadMessagesFromTopic(Connector connectSettings, string topic, string logpath)
        {
            Response response = new Response();

            response.Topic = topic;

            List<ResponseMessages> messagesResponse = new List<ResponseMessages>();

            var prefix = string.Format("ReadMessagesFromTopic. Topic: {0}. ", topic);

            Log(logpath, string.Format("{0}Старт процесса.", prefix));

            var config = new ConsumerConfig
            {
                GroupId = connectSettings.ConsumerGroupId,
                BootstrapServers = connectSettings.BootstrapServers,
                //SaslUsername = connectSettings.UserName,
                //SaslPassword = connectSettings.Password,
                //SaslMechanism = SaslMechanism,
                FetchMaxBytes = 1800000000,
                MessageMaxBytes = 1000000000,
                ReceiveMessageMaxBytes = 1850000000,
                SecurityProtocol = SecurityProtocol.Plaintext,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
                MaxPollIntervalMs = 10000,
                SessionTimeoutMs = 10000
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) =>
                {
                    Log(logpath, string.Format("{0}Ошибка подключения: {1}", prefix, e.Reason));
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
                                Log(logpath, string.Format("{0}Обработка сообщения: {1}.", prefix, cr.Offset));

                                ResponseMessages messageResponse = new ResponseMessages();

                                messageResponse.Created = cr.Message.Timestamp.UtcDateTime;
                                messageResponse.MessageId = cr.Offset.Value.ToString();
                                messageResponse.Value = cr.Message.Value;

                                Log(logpath, string.Format("{0}Сообщение с Id: {1} успешно обработано.", prefix, cr.Offset));

                                consumer.StoreOffset(cr);

                                messagesResponse.Add(messageResponse);
                            }
                            catch (ConsumeException ex)
                            {
                                Log(logpath, string.Format("{0}Во время обработки сообщения с Id: {1} произошла ошибка {2}.", prefix, cr.Offset, ex.Error));
                            }

                        }
                        else
                        {
                            Log(logpath, string.Format("{0}Нет доступных сообщений.", prefix));
                            consumer.Close();
                            break;
                        }
                    }
                }
                catch (OperationCanceledException ex)
                {
                    Log(logpath, string.Format("{0}При обработке возникла ошибка: {1}.", prefix, ex));
                }

                Log(logpath, string.Format("{0}Конец процесса.", prefix));
            }

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

            var config = new ConsumerConfig
            {
                GroupId = connectSettings.ConsumerGroupId,
                BootstrapServers = connectSettings.BootstrapServers,
                FetchMaxBytes = 1800000000,
                MessageMaxBytes = 1000000000,
                ReceiveMessageMaxBytes = 1850000000,
                SecurityProtocol = SecurityProtocol.Plaintext,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
                MaxPollIntervalMs = 10000,
                SessionTimeoutMs = 10000
            };

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
        /// Записать в лог файл сообщение.
        /// </summary>
        /// <param name="message">Сообщение.</param>
        public static void Log(string logPath, string message)
        {
            var text = string.Format("{0}   ({1})   {2}{3}", DateTime.Now, System.Diagnostics.Process.GetCurrentProcess().Id, message, Environment.NewLine);

            //если в конфиге папка не указана или указана, но папки такой нет, то использовать временную папку
            if (string.IsNullOrEmpty(logPath) || !System.IO.Directory.Exists(System.IO.Path.GetDirectoryName(logPath)))
            {
                logPath = System.IO.Path.GetTempPath();
            }
            if (!string.IsNullOrEmpty(logPath) && System.IO.Directory.Exists(System.IO.Path.GetDirectoryName(logPath)))
            {
                var serverName = Dns.GetHostName();

                var fileName = string.Format("{0}.KaffkaNet.{1}.log", serverName, DateTime.Today.ToShortDateString());

                var path = System.IO.Path.Combine(logPath, fileName);

                try
                {
                    if (!System.IO.File.Exists(path))
                        System.IO.File.WriteAllText(path, text);
                    else
                        System.IO.File.AppendAllText(path, text);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(string.Format("Log error: {0}", ex));
                }
            }
        }
    }

}
