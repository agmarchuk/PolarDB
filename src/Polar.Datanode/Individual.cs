using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
//using PolarDB;
using Polar.DB;

namespace Polar.Datanode
{
    /// <summary>
    /// Класс позволяет организовывать "индивидуальное" общение в среде узлов. Один узел, при этом, назначается мастером,
    /// остальные (клиенты) к нему подсоединяются. Мастеру позволено послать одиночный бинарный запрос и
    /// получить на него бинарный ответ, частным случаем бинарного ответа является пустое сообщение. 
    /// Запросы обрабатываются функцией, задаваемой для клиентского класса. 
    /// </summary>
    class Individual
    {
        private bool ismaster = false;
        private string masterhost;
        private string myhost = "0.0.0.0"; //"127.0.0.1";
        private int port;
        //private Tuple<PType, PType>[] signatures;
        private TableSignatures[] tass;
        // Команда преобразования пришедшей команды (com, tab, params) в объект
        private Func<int, int, object, object> execute_command;
        //private int nodecode = -1;
        //private Task portlistener = null;
        private List<IndividualChannel> masterchannels = new List<IndividualChannel>();
        public int NChannels { get { return masterchannels.Count; } }
        public void SetTass(TableSignatures[] tass) { this.tass = tass; }

        public Individual(bool ismaster, string masterhost, int port, TableSignatures[] tass, Func<int, int, object, object> execute_command)
        {
            this.ismaster = ismaster;
            this.masterhost = masterhost;
            this.port = port;
            this.tass = tass;
            this.execute_command = execute_command;
            if (ismaster)
            {
                // Запустить сервер (слушателя), ждать появления подсоединений
                Task servertask = StartServerAsync5(port);
                Task.Run(() => servertask);
                Console.WriteLine($"master is listenering port {port}");
            }
            else
            {
                // Для работника: законнектиться и выполнять цикл прием-выполнение-передача
                TcpClient client = new TcpClient();
                client.NoDelay = true;
                client.LingerState = new LingerOption(true, 0);
                Task connection = client.ConnectAsync(masterhost, port);
                Task.WaitAll(connection);
                Console.WriteLine($"connecting to {masterhost} port {port}");
                Task.Run(() => Worker(client));
            }
        }

        private async Task StartServerAsync5(int port)
        {
            IPAddress ipaddress = IPAddress.Parse(myhost);
            // Мастер вешает слушателя на свой компьютер
            TcpListener listener = new TcpListener(ipaddress, port);
            listener.Server.NoDelay = true;
            listener.Server.LingerState = new LingerOption(true, 0);
            listener.Start();
            // Цикл прослушивания: находятся коннекшины (клиенты) и собираются в словарь
            while (true)
            {
                TcpClient client = await listener.AcceptTcpClientAsync();
                client.NoDelay = true;
                client.LingerState = new LingerOption(true, 0);
                // Посылаем новый номер клиенту
                // ...
                // Получаем номер от клиента
                // ...
                // Сравниваем и фиксируем
                var stream = client.GetStream();
                var ic = new IndividualChannel(listener, client, stream);
                masterchannels.Add(ic);
                // пошлем номер канала, это и будет кодом узла
                int nom = masterchannels.Count - 1;
                //Order(nom, tp_command, nom, new PType(PTypeEnumeration.none));
            }
        }
        /// <summary>
        /// в канал посылается команда, у которой есть параметры: таблица и сообщение 
        /// </summary>
        /// <param name="chan"></param>
        /// <param name="command"></param>
        /// <param name="tab"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public object Order(int chan, int command, int tab, object message)
        {
            IndividualChannel ch = masterchannels[chan];
            // Определяем сигнатуру
            var typtyp = tass[tab].signatures[command];
            ch.bw.Write((byte)command);
            // посылаем таблицу
            ch.bw.Write(tab);
            // посылаем посылку
            //PaCell.SetPO(typtyp.Item1, ch.bw, message);
            ByteFlow.Serialize(ch.bw, message, typtyp.Item1);
            ch.Flush();
            object res = null;
            // Если нужно, принимаем результат
            if (typtyp.Item2.Vid != PTypeEnumeration.none)
            {
                res = ByteFlow.Deserialize(ch.br, typtyp.Item2); // PaCell.GetPO(typtyp.Item2, ch.br);
            }
            return res;
        }

        // Клиент в цикле ждет, получает команду, исполняет ее, возвращает результат
        public async Task Worker(TcpClient client)
        {
            using (Stream bstream = new BufferedStream(client.GetStream()))
            {
                System.IO.BinaryReader reader = new System.IO.BinaryReader(bstream);
                System.IO.BinaryWriter writer = new System.IO.BinaryWriter(bstream);
                //byte[] buff = new byte[1024];
                for (; ; )
                {
                    // Получаем команду
                    int command = reader.ReadByte();
                    //if (b == 255) b = reader.ReadInt32(); // Расширение системы команд 
                    // Принимаем таблицу
                    int tab = reader.ReadInt32();
                    var typtyp = tass[tab].signatures[command];
                    object ob = ByteFlow.Deserialize(reader, typtyp.Item1); //PaCell.GetPO(typtyp.Item1, reader);
                    object res = execute_command(command, tab, ob);
                    // проверка корректности пустого результата
                    if ((res == null) != (typtyp.Item2.Vid == PTypeEnumeration.none)) throw new Exception("Err: result is not null or sould ne null");
                    if (typtyp.Item2.Vid != PTypeEnumeration.none)
                    {
                        ByteFlow.Serialize(writer, res, typtyp.Item2); //PaCell.SetPO(typtyp.Item2, writer, res);
                        bstream.Flush();
                    }
                }
            }
        }

        //    public object Command()
        //    {
        //        if (masterchannels.Count > 0)
        //        {
        //            byte[] buff = new byte[4];
        //            IndividualChannel channel = masterchannels[0];
        //            try
        //            {
        //                // посылаем команду
        //                channel.bw.Write(0);
        //                //channel.stream.Write(buff, 0, 4);
        //                // Получаем ответ
        //                int b = channel.stream.ReadByte();
        //                if (b != 255) throw new Exception($"err: byte {b}");
        //            }
        //            catch (Exception ex) { Console.WriteLine(ex.Message); }
        //            return 1;
        //        }
        //        return null;
        //    }
    }

    /// <summary>
    /// Класс хранения информации об индивидуальном канале общения масета со слэйвом. Класс полностью готов для немедленного
    /// бинарного чтения из канала или бинарной записи в канал. 
    /// </summary>
    class IndividualChannel
    {
        //private bool _initiated = true;
        //public bool Initiated { get { return _initiated; } }
        private TcpListener listener;
        private TcpClient individual_client;
        private NetworkStream stream;
        private BufferedStream bstream;
        public BinaryWriter bw;
        public BinaryReader br;
        public IndividualChannel(TcpListener listener, TcpClient individual_client, NetworkStream stream)
        {
            this.listener = listener;
            this.individual_client = individual_client;
            this.stream = stream;
            this.bstream = new BufferedStream(stream);
            bw = new BinaryWriter(bstream);
            br = new BinaryReader(bstream);
        }
        public void Flush() { bstream.Flush(); }
    }

}
