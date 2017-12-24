using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polar.DB;
using PolarDB;
using UniversalIndex;
using Polar.PagedStreams;


namespace GetStarted
{
    public partial class Program
    {
        //static string path = "Databases/";
        public static void Main6()
        {
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start demo of table and Universal Index with PagedStorage and id as string");
            // Тип основной таблицы
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.sstring)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));

            // ======================== Страничное хранилище возьмем из описания PagedStreamStore =========================
            //PagedStreamStore ps_store = new PagedStreamStore(path + "storage.bin", 4); // заказали 4 стрима, конкретные будут: ps_store[i]
            StreamStorage ps_store = new StreamStorage(path + "storage6uindex.bin", 4); 

            // База данных такая же, как и в программе 4, только идентификатор определен как строка, придется воспользоваться полуключем
            TableView tab_person;
            IndexHalfkeyImmutable<string> index_id_arr;
            IndexDynamic<string, IndexHalfkeyImmutable<string>> index_id;
            // Подключение к таблице
            tab_person = new TableView(ps_store[0], tp_person);
            // Подключение к индексу по уникальному идентификатору (нулевое поле)
            Func<object, string> person_id_keyproducer = v => (string)((object[])((object[])v)[1])[0];
            index_id_arr = new IndexHalfkeyImmutable<string>(ps_store[1])
            {
                Table = tab_person,
                KeyProducer = person_id_keyproducer,
                HalfProducer = s => Hashfunctions.HashRot13(s),
                Scale = null
            };
            index_id_arr.Scale = new ScaleCell(ps_store[2]) { IndexCell = index_id_arr.IndexCell };
            index_id = new IndexDynamic<string, IndexHalfkeyImmutable<string>>(true)
            {
                Table = tab_person,
                IndexArray = index_id_arr,
                KeyProducer = person_id_keyproducer
            };
            tab_person.RegisterIndex(index_id);

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
                        return new object[] { id.ToString(), name, age };
                    });
                tab_person.Fill(flow);

                // Теперь надо отсортировать индексный массив по ключу
                tab_person.BuildIndexes();
                sw.Stop();
                Console.WriteLine("Load ok. duration for {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
            }
            else
            {
                // Загрузка кеша ускоряет обработку
                sw.Restart();
                ps_store.LoadCache();
                sw.Stop();
                Console.WriteLine("Cache load duration={0}", sw.ElapsedMilliseconds);
            }

            // Проверим работу
            int search_key = nelements * 2 / 3;
            var ob = index_id.GetAllByKey(search_key.ToString())
                .Select(ent => ((object[])ent.Get())[1])
                .FirstOrDefault();
            if (ob == null) throw new Exception("Didn't find person " + search_key);
            Console.WriteLine("Person {0} has name {1}", search_key, ((object[])ob)[1]);

            // Засечем скорость выборок
            int nprobes = 10000;
            sw.Restart();
            for (int i = 0; i < nprobes; i++)
            {
                search_key = rnd.Next(nelements) + 1;
                ob = index_id.GetAllByKey(search_key.ToString())
                    .Select(ent => ((object[])ent.Get())[1])
                    .FirstOrDefault();
                if (ob == null) throw new Exception("Didn't find person " + search_key);
                string nam = (string)((object[])ob)[1];
            }
            sw.Stop();
            Console.WriteLine("Duration for {0} search in {1} elements: {2} ms", nprobes, nelements, sw.ElapsedMilliseconds);
        }
    }
}
