using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polar.DB;

namespace GetStarted
{
    public partial class Program
    {
        //static string path = "Databases/";
        public static void Main12()
        {
            string path = "";
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start experiment with UniversalSequense");
            // Тип основной таблицы
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));
            Stream f1 = File.Open(path+"Databases/f1.bin", FileMode.OpenOrCreate);
            UniversalSequence<int> keyvalue_seq = new UniversalSequence<int>(tp_person, f1);
            // Теперь надо добавить индекс и использовать его
            Func<object[], int> keyFunc = (object[] re) => (int)re[0];
            // вводим индекс
            Stream f2 = File.Open(path+"Databases/f2.bin", FileMode.OpenOrCreate);
            Stream f3 = File.Open(path+"Databases/f3.bin", FileMode.OpenOrCreate);
            UniversalSequence<int> keys = new UniversalSequence<int>(new PType(PTypeEnumeration.integer), f2);
            UniversalSequence<long> offsets = new UniversalSequence<long>(new PType(PTypeEnumeration.longinteger), f3);

            int nelements = 10_000_000;
            Console.WriteLine($"Sequence of {nelements} elements");
            bool toload = true;
            int[] k_arr = new int[nelements];
            long[] o_arr = null;
            if (toload)
            {
                sw.Restart();
                keyvalue_seq.Clear();
                var query = Enumerable.Range(0, nelements).Select(i => new object[] { nelements - i - 1, "" + (nelements - i - 1), 33.3 });
                o_arr = new long[nelements];
                int pos = 0;
                foreach (var el in query)
                {
                    long off = keyvalue_seq.AppendElement(el);
                    k_arr[pos] = keyFunc(el);
                    o_arr[pos] = off;
                    pos++;
                }
                keyvalue_seq.Flush();
                // Сортировка
                Array.Sort(k_arr, o_arr);
                // Надо записать массивы
                keys.Clear();
                offsets.Clear();
                for (int i = 0; i < nelements; i++)
                {
                    keys.AppendElement(k_arr[i]);
                    offsets.AppendElement(o_arr[i]);
                }
                keys.Flush();
                offsets.Flush();
                o_arr = null;
                sw.Stop();
                Console.WriteLine($"Load ok. duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                // Надо прочитать массивы
                for (int i = 0; i < nelements; i++)
                {
                    object ob = i == 0 ? keys.GetElement(keys.ElementOffset(0L)) : keys.GetElement();
                    k_arr[i] = (int)ob;
                    //object ob2 = i == 0 ? offsets.GetElement(offsets.ElementOffset(0L)) : offsets.GetElement();
                    //o_arr[i] = (long)ob2;
                }
            }

            int k1 = nelements * 2 / 3;

            int nprobe = 10_000;

            sw.Restart();
            // Прямая выборка
            for (int i = 0; i < nprobe; i++)
            {
                int k2 = rnd.Next(nelements);
                int ind = Array.BinarySearch(k_arr, k2);
                //long offset = o_arr[ind];
                long offset = (long)offsets.GetElement(offsets.ElementOffset(ind));
                var r = (object[])keyvalue_seq.GetElement(offset);
            }
            sw.Stop();
            Console.WriteLine($"BinarySearch and GetElement ({nprobe} times). duration={sw.ElapsedMilliseconds}");

            /*
            // Собственно таблица
            TableView tab_person = new TableView(path + "person", tp_person);
            Func<object, int> person_code_keyproducer = v => (int)((object[])((object[])v)[1])[0];
            IndexKeyImmutable<int> ind_arr_person = new IndexKeyImmutable<int>(path + "person_ind")
            {
                Table = tab_person,
                KeyProducer = person_code_keyproducer,
                Scale = null
            };
            //ind_arr_person.Scale = new ScaleCell(path + "person_ind") { IndexCell = ind_arr_person.IndexCell };
            IndexDynamic<int, IndexKeyImmutable<int>> index_person = new IndexDynamic<int, IndexKeyImmutable<int>>(true)
            {
                Table = tab_person,
                IndexArray = ind_arr_person,
                KeyProducer = person_code_keyproducer
            };
            tab_person.RegisterIndex(index_person);

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

            // Проверим работу
            int search_key = nelements * 2 / 3;
            var ob = index_person.GetAllByKey(search_key)
                .Select(ent => ((object[])ent.Get())[1])
                .FirstOrDefault();
            if (ob == null) throw new Exception("Didn't find person " + search_key);
            Console.WriteLine("Person {0} has name {1}", search_key, ((object[])ob)[1]);

            // Засечем скорость выборок
            sw.Restart();
            for (int i = 0; i < 1000; i++)
            {
                search_key = rnd.Next(nelements) + 1;
                ob = index_person.GetAllByKey(search_key)
                    .Select(ent => ((object[])ent.Get())[1])
                    .FirstOrDefault();
                if (ob == null) throw new Exception("Didn't find person " + search_key);
                string nam = (string)((object[])ob)[1];
            }
            sw.Stop();
            Console.WriteLine("Duration for 1000 search in {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
            */
        }
    }
}
