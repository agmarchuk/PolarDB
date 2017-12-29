using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polar.DB;
using Polar.Cells;
using Polar.CellIndexes;
using Polar.PagedStreams;

namespace GetStarted
{
    public partial class Program
    {
        //static string path = "Databases/";
        public static void Main5()
        {
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start demo of table and Universal Index and PagedFileStore");
            // Тип основной таблицы
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));

            // ======================== Теперь нам понадобится страничное хранилище =========================
            // файл - носитель хранилища
            string dbpath = path + "storage3seq.bin";
            bool fob_exists = File.Exists(dbpath); 
            FileStream fs = new FileStream(dbpath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            FileOfBlocks fob = new FileOfBlocks(fs);
            Stream first_stream = fob.GetFirstAsStream();
            if (!fob_exists)
            {
                PagedStream.InitPagedStreamHead(first_stream, 8L, 0, PagedStream.HEAD_SIZE);
                fob.Flush();
            }
            PagedStream main_stream = new PagedStream(fob, fob.GetFirstAsStream(), 8L);

            long sz = PagedStream.HEAD_SIZE;
            // === В main_stream первым объектом поместим тествый поток
            // Если main_stream нулевой длины, надо инициировать конфигурацию стримов
            if (main_stream.Length == 0)
            {
                // инициируем 3 головы для потоков
                PagedStream.InitPagedStreamHead(main_stream, 0L, 0L, -1L);
                PagedStream.InitPagedStreamHead(main_stream, sz, 0L, -1L);
                PagedStream.InitPagedStreamHead(main_stream, 2 * sz, 0L, -1L);
                main_stream.Flush(); fob.Flush();
            }
            // создадим 3 потока
            PagedStream stream_person = new PagedStream(fob, main_stream, 0L);
            PagedStream stream_keys = new PagedStream(fob, main_stream, 1 * sz);
            PagedStream stream_offsets = new PagedStream(fob, main_stream, 2 * sz);



            // =========================== OK ==========================

            // Создадим базу данных, состоящую из последовательности, и двух индексных массивов: массива ключей и массива офсетов 
            PaCell person_seq = new PaCell(new PTypeSequence(tp_person), stream_person, false);
            Func<object, int> person_code_keyproducer = v => (int)((object[])v)[0];
            PaCell pers_ind_key_arr = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.integer)), stream_keys, false);
            PaCell pers_ind_off_arr = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.longinteger)), stream_offsets, false);

            int nelements = 1000000;
            bool toload = true; // Загружать или нет новую базу данных
            if (toload)
            {
                sw.Restart();
                // При загрузке кеш будет мешаться, его полезно деактивировать
                fob.DeactivateCache();
                // Очистим ячейку последовательности 
                person_seq.Clear();
                person_seq.Fill(new object[0]);

                // Для загрузки нам понадобятся: поток данных, массив ключей и массив офсетов (это всего лишь демонстрация!)
                IEnumerable<object> flow = Enumerable.Range(0, nelements)
                    .Select(i =>
                    {
                        int id = nelements - i;
                        string name = "=" + id.ToString() + "=";
                        double age = rnd.NextDouble() * 100.0;
                        return new object[] { id, name, age };
                    });
                int[] key_arr = new int[nelements];
                long[] off_arr = new long[nelements];

                int ind = 0;
                foreach (object[] orec in flow)
                {
                    long off = person_seq.Root.AppendElement(orec);
                    int key = (int)(((object[])orec)[0]);
                    key_arr[ind] = key;
                    off_arr[ind] = off;
                    ind++;
                }
                person_seq.Flush();
                Console.WriteLine("Sequence OK.");
                // Параллельная сортировка по значению ключа
                Array.Sort(key_arr, off_arr);

                pers_ind_key_arr.Clear();
                pers_ind_key_arr.Fill(new object[0]);
                for (int i = 0; i < nelements; i++) pers_ind_key_arr.Root.AppendElement(key_arr[i]);
                pers_ind_key_arr.Flush();

                pers_ind_off_arr.Clear();
                pers_ind_off_arr.Fill(new object[0]);
                for (int i = 0; i < nelements; i++) pers_ind_off_arr.Root.AppendElement(off_arr[i]);
                pers_ind_off_arr.Flush();

                fob.Flush();

                sw.Stop();
                Console.WriteLine("Load ok. duration for {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
            }

            sw.Restart();
            fob.LoadCache();
            sw.Stop();
            Console.WriteLine("Cache load duration={0}", sw.ElapsedMilliseconds);

            sw.Restart();
            int nkeys = (int)pers_ind_key_arr.Root.Count();
            int[] keys = new int[nkeys];
            int ii = 0;
            foreach (int k in pers_ind_key_arr.Root.ElementValues()) { keys[ii] = k; ii++; }

            sw.Stop();
            Console.WriteLine("Index preload duration={0}", sw.ElapsedMilliseconds);

            sw.Restart();

            PaEntry entry = person_seq.Root.Element(0);
            int nte = 100000;
            for (int i = 0; i < nte; i++)
            {
                int key = rnd.Next(nkeys);
                int ifirst = GetFirstIndexOf(0, nelements, keys, key);
                long offset = (long)pers_ind_off_arr.Root.Element(ifirst).Get();
                entry.offset = offset;
                object[] rec = (object[])entry.Get();
                //Console.WriteLine($"{rec[0]} {rec[1]} {rec[2]}");
            }
            sw.Stop();
            Console.WriteLine("Duration={0} for {1}", sw.ElapsedMilliseconds, nte);

            //// Проверим работу
            //int search_key = nelements * 2 / 3;
            //var ob = index_person.GetAllByKey(search_key)
            //    .Select(ent => ((object[])ent.Get())[1])
            //    .FirstOrDefault();
            //if (ob == null) throw new Exception("Didn't find person " + search_key);
            //Console.WriteLine("Person {0} has name {1}", search_key, ((object[])ob)[1]);

            //// Засечем скорость выборок
            //sw.Restart();
            //for (int i = 0; i < 1000; i++)
            //{
            //    search_key = rnd.Next(nelements);
            //    ob = index_person.GetAllByKey(search_key)
            //        .Select(ent => ((object[])ent.Get())[1])
            //        .FirstOrDefault();
            //    if (ob == null) throw new Exception("Didn't find person " + search_key);
            //    string nam = (string)((object[])ob)[1];
            //}
            //sw.Stop();
            //Console.WriteLine("Duration for 1000 search in {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
        }
        private static int GetFirstIndexOf(int start, int number, int[] keys, int key)
        {
            int half = number / 2;
            if (half == 0) return -1; // Не найден
            int middle = start + half;
            var middle_depth = keys[middle].CompareTo(key);
            if (middle_depth == 0) return middle;
            if (middle_depth < 0)
            {
                return GetFirstIndexOf(middle, number - half, keys, key);
            }
            else
            {
                return GetFirstIndexOf(start, half, keys, key);
            }
        }
    }
}
