using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Polar.PagedStreams
{
    public class PagedStreamStore
    {
        private PagedStream[] pstreams;
        public PagedStream this[int i]
        {
            get { return pstreams[i]; }
        }
        private FileOfBlocks fob;
        public PagedStreamStore(string dbpath, int nstreams)
        {
            bool fob_exists = File.Exists(dbpath);
            // Открываем или создаем файл-носитель хранилища
            FileStream fs = new FileStream(dbpath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            // Создаем собственно блочное (страничное) хранилище
            fob = new FileOfBlocks(fs);
            // Далее идет корявый способ создания трех потоков (Stream), нужных для базы данных 
            Stream first_stream = fob.GetFirstAsStream();
            if (!fob_exists)
            {
                PagedStream.InitPagedStreamHead(first_stream, 8L, 0, PagedStream.HEAD_SIZE);
                fob.Flush();
            }
            PagedStream main_stream = new PagedStream(fob, fob.GetFirstAsStream(), 8L);
            long sz = PagedStream.HEAD_SIZE;
            // Если main_stream нулевой длины, надо инициировать конфигурацию стримов
            if (main_stream.Length == 0)
            {
                // инициируем nstreams голов для потоков
                for (int i=0; i<nstreams; i++)
                {
                    PagedStream.InitPagedStreamHead(main_stream, i * sz, 0L, -1L);
                }
                main_stream.Flush(); fob.Flush();
            }
            // создадим nstreams потоков
            pstreams = new PagedStream[nstreams];
            for (int i = 0; i < nstreams; i++)
            {
                pstreams[i] = new PagedStream(fob, main_stream, i * sz);
            }
        }
        public void ActivateCache() { fob.ActivateCache(); }
        public void DeactivateCache() { fob.DeactivateCache(); }
        public void LoadCache() { fob.LoadCache(); }
    }
}
