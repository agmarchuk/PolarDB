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
        public static void Main4()
        {
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start demo of table and Universal Index with PagedStorage");
            // Тип основной таблицы
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));

            // ======================== Теперь нам понадобится страничное хранилище =========================
            // файл - носитель хранилища
            string filepath = dbpath + "storage.bin";
            bool fob_exists = File.Exists(filepath); // этот признак используется при разных процессах инициализации
            // Открываем иили создаем файл-носитель хранилища
            FileStream fs = new FileStream(filepath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            // Создаем собственно блочное (страничное) хранилище
            FileOfBlocks fob = new FileOfBlocks(fs);
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
                // инициируем 3 головы для потоков
                PagedStream.InitPagedStreamHead(main_stream, 0L, 0L, -1L);
                PagedStream.InitPagedStreamHead(main_stream, sz, 0L, -1L);
                PagedStream.InitPagedStreamHead(main_stream, 2 * sz, 0L, -1L);
                main_stream.Flush(); fob.Flush();
            }
            // создадим 3 потока
            PagedStream stream_person = new PagedStream(fob, main_stream, 0L);
            PagedStream stream_index = new PagedStream(fob, main_stream, 1 * sz);
            PagedStream stream_scale = new PagedStream(fob, main_stream, 2 * sz);
            // =========================== OK ==========================

            // База данных такая же, как и в программе 3, только вместо файлов используются потоки
            TableView tab_person = new TableView(stream_person, tp_person);
            Func<object, int> person_code_keyproducer = v => (int)((object[])((object[])v)[1])[0];
            IndexKeyImmutable<int> ind_arr_person = new IndexKeyImmutable<int>(stream_index)
            {
                Table = tab_person,
                KeyProducer = person_code_keyproducer,
                Scale = null
            };
            ind_arr_person.Scale = new ScaleCell(stream_scale) { IndexCell = ind_arr_person.IndexCell };
            IndexDynamic<int, IndexKeyImmutable<int>> index_person = new IndexDynamic<int, IndexKeyImmutable<int>>(true)
            {
                Table = tab_person,
                IndexArray = ind_arr_person,
                KeyProducer = person_code_keyproducer
            };
            tab_person.RegisterIndex(index_person);

            fob.DeactivateCache();

            int nelements = 1000000;
            bool toload = true; // Загружать или нет новую базу данных
            if (toload)
            {
                sw.Restart();
                // Очистим ячейки последовательности и индекса 
                tab_person.Clear();

                IEnumerable<object> flow = Enumerable.Range(0, nelements)
                    .Select(i =>
                    {
                        int id = nelements - i;
                        string name = "=" + id.ToString() + "=";
                        double age = rnd.NextDouble() * 100.0;
                        return new object[] { id, name, age };
                    });
                tab_person.Fill(flow);

                // Теперь надо отсортировать индексный массив по ключу
                tab_person.BuildIndexes();
                sw.Stop();
                Console.WriteLine("Load ok. duration for {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
            }

            sw.Restart();
            // Загрузка кеша ускоряет обработку
            fob.LoadCache();
            sw.Stop();
            Console.WriteLine("Cache load duration={0}", sw.ElapsedMilliseconds);

            // Проверим работу
            int search_key = nelements * 2 / 3;
            var ob = index_person.GetAllByKey(search_key)
                .Select(ent => ((object[])ent.Get())[1])
                .FirstOrDefault();
            if (ob == null) throw new Exception("Didn't find person " + search_key);
            Console.WriteLine("Person {0} has name {1}", search_key, ((object[])ob)[1]);

            // Засечем скорость выборок
            int nte = 10000;
            sw.Restart();
            for (int i = 0; i < nte; i++)
            {
                search_key = rnd.Next(nelements) + 1;
                ob = index_person.GetAllByKey(search_key)
                    .Select(ent => ((object[])ent.Get())[1])
                    .FirstOrDefault();
                if (ob == null) throw new Exception("Didn't find person " + search_key);
                string nam = (string)((object[])ob)[1];
            }
            sw.Stop();
            Console.WriteLine("Duration for {2} search in {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds, nte);
        }
    }
}
