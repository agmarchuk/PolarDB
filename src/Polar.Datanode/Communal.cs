using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Datanode
{
    /// <summary>
    /// Класс позволяет организовывать коммунальное общение в среде узлов. Один узел, при этом, назначается мастером,
    /// остальные (клиенты) пользуются его услугими. Клиентскому узлу позволено послать одиночный текстовый запрос и
    /// получить на него текстовый ответ. Кодировка текстов - UTF-8. Запросы обрабатываются функцией, задаваемой для
    /// класса. 
    /// </summary>
    class Communal
    {
        private string host;
        private int port;
        private bool isboss = false;
        private Task portlistener = null;
        public Communal(string host, int port, bool isboss)
        {
            this.host = host;
            this.port = port;
            this.isboss = isboss;
            if (isboss)
            {
                if (portlistener == null)
                {
                    portlistener = StartServerAsync4(port, mess =>
                    {
                        // Здесь должен располагаться анализ запроса и выработка ответа!
                        return $"boss got [{mess}]";
                    });
                    Task.Run(() => portlistener);
                }
            }
        }

        // Клиент только запрашивает, запрос может сколько-то длиться. 
        public async Task<string> AskBoss(string request)
        {
            string response = "noresponse";
            using (TcpClient client = new TcpClient())
            {
                client.NoDelay = true;
                client.LingerState = new LingerOption(true, 0);
                await client.ConnectAsync(host, port);
                using (NetworkStream stream = client.GetStream())
                {
                    // Посылаем
                    byte[] arr = System.Text.Encoding.ASCII.GetBytes(request);
                    stream.Write(arr, 0, arr.Length);
                    // Принимаем
                    System.IO.TextReader reader = new System.IO.StreamReader(stream, System.Text.Encoding.UTF8);
                    response = reader.ReadToEnd();
                }
            }
            return response;
        }
        private static string myhost = "0.0.0.0";
        private static async Task StartServerAsync4(int port, Func<string, string> decide)
        {
            IPAddress ipaddress = IPAddress.Parse(myhost);
            // Слушатель вешается на свой компьютер
            TcpListener listener = new TcpListener(ipaddress, port);
            listener.Server.NoDelay = true;
            listener.Server.LingerState = new LingerOption(true, 0);
            listener.Start();

            while (true)
            {
                using (TcpClient client = await listener.AcceptTcpClientAsync())
                {
                    client.NoDelay = true;
                    client.LingerState = new LingerOption(true, 0);
                    using (NetworkStream stream = client.GetStream())
                    {
                        string received = null;

                        // Принимаем запрос
                        byte[] buff = new byte[1024];
                        int nbytes = stream.Read(buff, 0, buff.Length);
                        received = System.Text.Encoding.UTF8.GetString(buff, 0, nbytes);

                        // Анализируем и принимаем решение
                        string decision = decide(received);

                        // Сообщаем
                        byte[] resp = System.Text.Encoding.UTF8.GetBytes(decision);
                        stream.Write(resp, 0, resp.Length);
                    }
                }
            }
        }

    }
}
