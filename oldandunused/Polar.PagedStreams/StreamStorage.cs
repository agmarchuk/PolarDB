using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Polar.PagedStreams
{
    public class StreamStorage
    {
        private List<Stream> streams = new List<Stream>();

        public int Count() { return streams.Count; }
        public Stream this[int i]
        {
            get { return streams[i]; }
        }
        private FileOfBlocks fob;
        private PagedStream collection_stream;
        private long sz;
        public StreamStorage(string dbpath)
        {

            bool fob_exists = File.Exists(dbpath);
            // Открываем или создаем файл-носитель хранилища
            FileStream fs = new FileStream(dbpath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            // Создаем собственно блочное (страничное) хранилище
            fob = new FileOfBlocks(fs);

            fob.DeactivateCache();

            sz = PagedStream.HEAD_SIZE;
            // Далее идет корявый способ создания трех потоков (Stream), нужных для базы данных 
            Stream first_stream = fob.GetFirstAsStream();
            if (!fob_exists)
            {
                PagedStream.InitPagedStreamHead(first_stream, 8L, 0, PagedStream.HEAD_SIZE);
                fob.Flush();
            }
            PagedStream main_stream = new PagedStream(fob, fob.GetFirstAsStream(), 8L);

            // Если main_stream нулевой длины, надо инициировать конфигурацию стримов
            bool toinit = main_stream.Length == 0;
            if (toinit)
            {
                // инициируем 2 головы для потоков
                PagedStream.InitPagedStreamHead(main_stream, 0L, 0L, -1L); // Это будет для коллекций
                PagedStream.InitPagedStreamHead(main_stream, PagedStream.HEAD_SIZE, 0L, -1L);
                main_stream.Flush(); fob.Flush();
            }
            collection_stream = new PagedStream(fob, main_stream, 0L);
            PagedStream special_stream = new PagedStream(fob, main_stream, sz); //TODO: резерв 

            // Если main_stream ненулевой длины, надо инициировать имеющуюся конфигурацию стримов
            if (collection_stream.Length > 0)
            {
                int nstreams = (int)(collection_stream.Length / sz);
                // инициируем nstreams голов для потоков
                for (int i = 0; i < nstreams; i++)
                {
                    //PagedStream.InitPagedStreamHead(main_stream, i * sz, 0L, -1L);
                    streams.Add(new PagedStream(fob, collection_stream, i * sz));
                }
                collection_stream.Flush(); fob.Flush();
            }
        }
        public StreamStorage(string dbpath, int nstreams) : this(dbpath)
        {
            int nn = -1;
            for (int i = 0; i < nstreams; i++) this.CreateStream(out nn);
        }
        public Stream CreateStream(out int number)
        {
            number = Count();
            PagedStream.InitPagedStreamHead(collection_stream, number * sz, 0L, -1L);
            collection_stream.Flush(); fob.Flush();
            Stream created = new PagedStream(fob, collection_stream, number * sz);
            streams.Add(created);
            return created;
        }
        public void Flush() { fob.Flush(); }
        public void Close() { fob.Close(); }

        public void ActivateCache() { fob.ActivateCache(); }
        public void DeactivateCache() { fob.DeactivateCache(); }
        public void LoadCache() { fob.LoadCache(); }
    }
}
