using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
//using PolarDB;

namespace Polar.Datanode
{
    /// <summary>
    /// Класс позволяет организовывать имитацию "индивидуального" общения в среде узлов. Один узел, при этом, назначается мастером,
    /// остальные (исполнители или слэйвы) к нему подсоединяются. Мастеру позволено послать одиночный бинарный запрос и
    /// получить на него бинарный ответ, частным случаем бинарного ответа является пустое сообщение. 
    /// Запросы обрабатываются функцией, задаваемой для клиентского класса. 
    /// </summary>
    class IndividualSpeed
    {
        private bool ismaster = false;
        private string masterhost;
        private string myhost = "0.0.0.0"; //"127.0.0.1";
        private int port;
        private List<Stream> client_streams = new List<Stream>();

        public IndividualSpeed(bool ismaster, string masterhost, int port)
        {
            this.ismaster = ismaster;
            this.masterhost = masterhost;
            this.port = port;
            if (ismaster)
            {
                // Запустить сервер (слушателя), ждать появления подсоединений
                Task servertask = StartServerAsync6(port);
                Task.Run(() => servertask);
                Console.WriteLine($"ISmaster is listening port {port}");
            }
            else
            {
                // Для работника: законнектиться и выполнять цикл прием-выполнение-передача
                TcpClient client = new TcpClient();
                client.NoDelay = true;
                client.LingerState = new LingerOption(true, 0);
                Task connection = client.ConnectAsync(masterhost, port);
                Task.WaitAll(connection);
                Console.WriteLine($"ISconnecting to {masterhost} port {port}");
                Task.Run(() => Worker(client));
            }
        }

        private async Task StartServerAsync6(int port)
        {
            IPAddress ipaddress = IPAddress.Parse(myhost);
            // Мастер вешает слушателя на свой компьютер
            TcpListener listener = new TcpListener(ipaddress, port);
            listener.Server.NoDelay = true;
            listener.Server.LingerState = new LingerOption(true, 0);
            listener.Start();
            // Цикл прослушивания: находятся коннекшины (клиенты) и собираются в список
            while (true)
            {
                TcpClient client = await listener.AcceptTcpClientAsync();
                client.NoDelay = true;
                client.LingerState = new LingerOption(true, 0);
                Console.Write("client is connected\n>>");
                var stream = client.GetStream();
                client_streams.Add(stream);
            }
        }
        private byte[] requ_arr = new byte[12];
        private byte[] resp_arr = new byte[1024];

        public void TestOrder()
        {
            // годится только для мастера
            Stream stream = client_streams[0];
            
            // Послать
            stream.Write(requ_arr, 0, 12);

            // Принять
            //int nb = stream.Read(resp_arr, 0, 64); //resp_arr.Length);
            int rlen = 64;
            ReadToArr(stream, resp_arr, rlen);

            //Console.WriteLine($"{nb} bytes received");
        }

        private void ReadToArr(Stream stream, byte[] buff, int len)
        {
            int cnt = 0;
            while (cnt != len)
            {
                int c = stream.Read(buff, cnt, len - cnt);
                if (c == 0) throw new Exception($"Err in ReadToArr: {cnt} bytes read instead {len}");
                cnt += c;
            }
        }

        // Клиент в цикле ждет, получает команду, исполняет ее, возвращает результат
        public async Task Worker(TcpClient client)
        {
            using (Stream stream = client.GetStream())
            {
                byte[] buff = new byte[1024];
                byte[] result_arr = new byte[64]; // имитация записи int, string, int 
                for (; ; )
                {
                    // Получаем команду
                    //int nb = stream.Read(buff, 0, 12); //buff.Length);
                    ReadToArr(stream, buff, 12);

                    //Console.WriteLine($"{nb} bytes received");
                    // Как-бы выполянем и возвращаем результат
                    stream.Write(result_arr, 0, 64); //result_arr.Length);
                    //Console.WriteLine($"{result_arr.Length} bytes sent");
                }
            }
        }
    }
}
