using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polar.DB;
//using PolarDB;
//using UniversalIndex;
//using PagedFileStore;
using Polar.Cells;
using Polar.PagedStreams;

namespace GetStarted
{
    public partial class Program
    {
        public static void Main8_1()
        {
            int max = 1000000;
            int[] arr = Enumerable.Range(1, max).ToArray();
            var Dia = GetDiaFunc2(arr);
            foreach (int key in arr)
            {
                Diapason di = Dia(key);
                if (di.IsEmpty()) Console.WriteLine($"no dia for {key}");
                int start = (int)di.start;
                int numb = (int)di.numb;

                int found = -1;
                for (int i = start; i < start + numb || i < arr.Length; i++)
                {
                    if (arr[i] == key) { found = i; break; }
                }
                if (found == -1) Console.WriteLine($"not found for {key}: start={start} numb={numb}");

                int ind = Program.Get8FirstIndexOf(start, numb, arr, key);
                if (ind == -1)
                {
                    Console.WriteLine($"no ind for key {key}");
                    int ind2 = Program.Get8FirstIndexOf(start, numb, arr, key);
                }
                else
                {
                    int keyval = arr[ind];
                    if (keyval != key)
                    {
                        Console.WriteLine($"bad result index {ind} {keyval} for {key}");
                    }
                }
            }
            Console.WriteLine("ok?");
        }
        internal static Func<int, Diapason> GetDiaFunc2(int[] keys)
        {
            // Построение шкалы
            int N = keys.Length;
            int V_min = keys[0];
            int V_max = keys[N - 1];
            int M = N / 16;
            int[] scale = new int[M];
            int index = 0;
            int ik = 0;
            scale[index] = ik;

            Func<int, int> IndxTokley = indx => (int)((long)V_min + (((long)(index+1) * (long)(V_max - V_min) / (long)M)));

            for (; ik < N; ik++)
            {
                int key = IndxTokley(ik);
                if (keys[ik] < key) { continue; }
                index = index + 1;

                if (index == M) { break; }
                scale[index] = ik+1;
            }

            int last = scale[scale.Length - 1];

            Func<int, Diapason> GetDia = key =>
            {
                int indx = (int)((long)M * (long)(key - V_min) / (long)(V_max - V_min));
                if (indx == M) return new Diapason() { start = N - 1, numb = 1 };
                if (indx == M - 1) return new Diapason() { start = scale[indx], numb = N - scale[indx] };
                int sta = scale[indx];
                int sto = scale[indx + 1];
                var d = new Diapason() { start = scale[indx], numb = scale[indx + 1] - scale[indx] };
                return d;
            };
            return GetDia;
        }


        private static object locker = new object();
        //static string path = "Databases/";
        public static void Main8_()
        {
            Random rnd = new Random(); //new Random(777);
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start demo key-value storage");
            // Тип основной таблицы
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));
            PType tp_index = new PTypeSequence(new PTypeRecord(
                new NamedType("key", new PType(PTypeEnumeration.integer)),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))
                ));

            // ======================== Теперь нам понадобится страничное хранилище =========================
            // файл - носитель хранилища
            string dbpath = path + "storage8.bin";
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
                //PagedStream.InitPagedStreamHead(main_stream, 3 * sz, 0L, -1L);
                main_stream.Flush(); fob.Flush();
            }
            // создадим 3 потока
            PagedStream stream_person = new PagedStream(fob, main_stream, 0L);
            PagedStream stream_keys = new PagedStream(fob, main_stream, 1 * sz);
            PagedStream stream_offsets = new PagedStream(fob, main_stream, 2 * sz);
            //PagedStream stream_index = new PagedStream(fob, main_stream, 3 * sz);



            // =========================== OK ==========================

            // Создадим базу данных, состоящую из последовательности, и двух индексных массивов: массива ключей и массива офсетов 
            PaCell person_seq = new PaCell(new PTypeSequence(tp_person), stream_person, false);
            Func<object, int> person_code_keyproducer = v => (int)((object[])v)[0];
            PaCell pers_ind_key_arr = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.integer)), stream_keys, false);
            PaCell pers_ind_off_arr = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.longinteger)), stream_offsets, false);

            int nelements = 10000000;
            bool toload = false; // Загружать или нет новую базу данных
            if (toload)
            {
                FileStream tmp_file = new FileStream(path + "tmp.pac", FileMode.CreateNew, FileAccess.ReadWrite);
                PaCell key_index = new PaCell(tp_index, tmp_file, false);
                sw.Restart();
                // При загрузке кеш будет мешаться, его полезно деактивировать
                fob.DeactivateCache();
                // Очистим ячейку последовательности 
                person_seq.Clear();
                person_seq.Fill(new object[0]);

                // Для загрузки нам понадобятся: поток данных, массив ключей и массив офсетов (это всего лишь демонстрация!)
                IEnumerable<object> flow = Enumerable.Range(0, nelements)
                    .Select(iv =>
                    {
                        int id = nelements - iv;
                        string name = "=" + id.ToString() + "=";
                        double age = rnd.NextDouble() * 100.0;
                        return new object[] { id, name, age };
                    });
                //int[] key_arr = new int[nelements];
                //long[] off_arr = new long[nelements];

                key_index.Clear();
                key_index.Fill(new object[0]);
                //int ind = 0;
                foreach (object[] orec in flow)
                {
                    long off = person_seq.Root.AppendElement(orec);
                    int key = (int)(((object[])orec)[0]);
                    key_index.Root.AppendElement(new object[] { key, off });
                }
                person_seq.Flush();
                key_index.Flush();

                Console.WriteLine("Sequence OK. duration={0}", sw.ElapsedMilliseconds);

                // Сортировка по значению ключа
                key_index.Root.SortByKey<int>(ob => (int)((object[])ob)[0]);

                //TODO: Надо "сжать" индексный файл, убрав из него дубли по ключам

                pers_ind_key_arr.Clear(); pers_ind_key_arr.Fill(new object[0]);
                foreach (object[] ob in key_index.Root.ElementValues())
                {
                    pers_ind_key_arr.Root.AppendElement(ob[0]);
                }
                pers_ind_key_arr.Flush();

                pers_ind_off_arr.Clear(); pers_ind_off_arr.Fill(new object[0]);
                foreach (object[] ob in key_index.Root.ElementValues())
                {
                    pers_ind_off_arr.Root.AppendElement(ob[1]);
                }
                pers_ind_off_arr.Flush();

                fob.Flush();

                sw.Stop();
                Console.WriteLine("Load ok. duration for {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
                key_index.Close();
                tmp_file.Dispose();
                File.Delete(path + "tmp.pac");
            }

            sw.Restart();
            fob.ActivateCache();
            fob.LoadCache();
            sw.Stop();
            Console.WriteLine("Cache load duration={0}", sw.ElapsedMilliseconds);

            sw.Restart();
            //int nkeys = (int)pers_ind_key_arr.Root.Count();
            int[] keys = pers_ind_key_arr.Root.ElementValues().Cast<int>().ToArray();
            //long[] offsets = key_index.Root.ElementValues().Cast<object[]>().Select(ob => (long)ob[1]).ToArray();

            sw.Stop();
            Console.WriteLine("Index preload duration={0}", sw.ElapsedMilliseconds);

            Func<int, Diapason> GetDia = GetDiaFunc2(keys);

            PaEntry entry = person_seq.Root.Element(0);
            int search_key = nelements * 2 / 3;

            ComputeEntry(pers_ind_off_arr, keys, GetDia, entry, search_key);

            object[] re = (object[])entry.Get();
            Console.WriteLine($"{re[0]} {re[1]} {re[2]}");

            int nte = 100000;
            rnd = new Random();

            sw.Restart();
            for (int i = 0; i < nte; i++)
            {
                int key = rnd.Next(keys.Length) + 1;
                Diapason diap = GetDia(key);
                int ifirst = Get8FirstIndexOf((int)diap.start, (int)diap.numb, keys, key);
                if (ifirst == -1)
                {
                    entry = PaEntry.Empty;
                    Console.WriteLine($"index not found for ...");
                    continue;
                }
                entry.offset = (long)pers_ind_off_arr.Root.Element(ifirst).Get();
                object[] rec = (object[])entry.Get();
            }
            sw.Stop();
            Console.WriteLine("request test duration={0} for {1}", sw.ElapsedMilliseconds, nte);
        }

        //internal static Func<int, Diapason> GetDiaFunc(int[] keys)
        //{
        //    // Построение шкалы
        //    int N = keys.Length;
        //    int V_min = keys[0];
        //    int V_max = keys[N - 1];
        //    int M = N / 16;
        //    int[] scale = new int[M];
        //    int index = 0;
        //    int ik = 0;
        //    scale[index] = ik;

        //    for (; ik < N; ik++)
        //    {
        //        if (keys[ik] < (int)((long)V_min + ((long)index + 1L) * ((long)V_max - (long)V_min) / (long)M)) { continue; }
        //        index = index + 1;

        //        if (index == M) { break; }
        //        scale[index] = ik;
        //    }

        //    Func<int, Diapason> GetDia = key =>
        //    {
        //        int indx = (int)((long)M * (long)(key - V_min) / (long)(V_max - V_min));
        //        if (indx == M) return new Diapason() { start = N - 1, numb = 1 };
        //        if (indx == M - 1) return new Diapason() { start = scale[indx], numb = N - scale[indx] };
        //        int sta = scale[indx];
        //        int sto = scale[indx + 1];
        //        var d = new Diapason() { start = scale[indx], numb = scale[indx + 1] - scale[indx] };
        //        return d;
        //    };
        //    return GetDia;
        //}

        private static void ComputeEntry(PaCell pers_ind_off_arr, int[] keys, Func<int, Diapason> GetDia, PaEntry entry, int search_key)
        {
            Diapason diap = GetDia(search_key);
            int ifirst = Get8FirstIndexOf((int)diap.start, (int)diap.numb, keys, search_key);
            if (ifirst == -1) { entry = PaEntry.Empty; return; }
            //entry.offset = offsets[ifirst]; // Использование массива 
            //entry.offset = (long)key_index.Root.Element(ifirst).Field(1).Get(); // Использование всего индекса  
            entry.offset = (long)pers_ind_off_arr.Root.Element(ifirst).Get();
        }


        public static int Get8FirstIndexOf(int start, int number, int[] keys, int key)
        {
            int half = number / 2;
            if (number < 1)
            {
                return -1;
            } // Не найден
            int middle = start + half;
            var middle_depth = keys[middle].CompareTo(key);

            if (middle_depth == 0) return middle;
            if (middle_depth < 0)
            {
                //return Get8FirstIndexOf(middle + 1, number - half - 1, keys, key);
                return Get8FirstIndexOf(middle + 1, number - half - 1, keys, key);
            }
            else
            {
                return Get8FirstIndexOf(start, half, keys, key);
            }

            //var qu = Enumerable.Range(start, number).Where(i => keys[i].CompareTo(key) == 0);
            //int ind = -1;
            //foreach (int ii in qu) { ind = ii; break; }
            //return ind;
        }

        public static void Main8()
        {
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start demo key-value DataNode + SequencePortion");
            DataNode dn = new DataNode(path);
            PType tp_person = new PTypeRecord(
                    new NamedType("name", new PType(PTypeEnumeration.sstring)),
                    new NamedType("age", new PType(PTypeEnumeration.real)));
            //PType tp_configuration =
            //    new PTypeSequence(new PTypeRecord(
            //        new NamedType("tab_name", new PType(PTypeEnumeration.sstring)),
            //        new NamedType("key_type", PType.TType),
            //        new NamedType("value_type", PType.TType),
            //        new NamedType("key_grades", new PTypeSequence(new PTypeRecord(
            //            new NamedType("portion_resource", new PType(PTypeEnumeration.integer))
            //            )))
            //        ));

            if (dn.NPortions == 0)
            {
                object commondata = new object[] {
                    // конфигуратор
                    new object[] {
                        // Первая таблица
                        new object[] { "persons", new PType(PTypeEnumeration.integer).ToPObject(1), tp_person.ToPObject(3),
                            Enumerable.Repeat<object[]>(new object[1], 16).ToArray()
                    } }
                };
                dn.ConfigNode(commondata);
            }

            dn.fob.DeactivateCache();
            //KVSequencePortion portion = dn.Portion(0);
            int nelements = 1000000;
            bool toload = true; // Загружать или нет новую базу данных
            if (toload)
            {
                dn.fob.DeactivateCache();
                for (int i = 0; i < dn.NPortions; i++)
                {
                    dn.Portion(i).Clear();
                }
                bool tododynamicindex = false;
                sw.Restart();
                // Для загрузки нам понадобятся: поток данных, массив ключей и массив офсетов (это всего лишь демонстрация!)
                IEnumerable<object[]> flow = Enumerable.Range(0, nelements)
                    .Select(iv =>
                    {
                        int id = nelements - iv;
                        string name = "=" + id.ToString() + "=";
                        double age = rnd.NextDouble() * 100.0;
                        return new object[] { id, new object[] { name, age }  };
                    });
                long cnt = 1;
                foreach (object[] pair in flow)
                {
                    //int ke = (int)pair[0];
                    //long offset = dn.Portion(ke % 4).AppendPair(pair, tododynamicindex);
                    dn.Append(0, pair, tododynamicindex);

                    //if (cnt % 100000 == 0) Console.Write($"{cnt} ");
                    cnt++;
                }
                //Console.WriteLine();
                for (int i = 0; i < dn.NPortions; i++)
                {
                    dn.Portion(i).Flush();
                }
                sw.Stop();
                if (tododynamicindex)
                {
                    Console.WriteLine($"CalculateStaticIndex {sw.ElapsedMilliseconds}");
                }
                else
                {
                    Console.WriteLine($"load {sw.ElapsedMilliseconds}");
                    sw.Restart();
                    for (int i = 0; i < dn.NPortions; i++)
                    {
                        dn.Portion(i).CalculateStaticIndex();
                    }
                    sw.Stop();
                    Console.WriteLine($"CalculateStaticIndex {sw.ElapsedMilliseconds}");
                }
                dn.Flush();
            }
            else
            {
                
                Console.WriteLine($"NOload {sw.ElapsedMilliseconds}");
            }

            sw.Restart();
            for (int i = 0; i < dn.NPortions; i++) dn.Portion(i).Activate();
            sw.Stop();
            Console.WriteLine($"activate array {sw.ElapsedMilliseconds}");

            sw.Restart();
            //dn.fob.ActivateCache();
            //dn.fob.LoadCache();

            sw.Stop();
            Console.WriteLine($"load cache {sw.ElapsedMilliseconds}");

            int key = nelements * 2 / 3;
            object[] testvalue = (object[])dn.Get(0, key); //(object[])dn.Portion(key % 4).Get(key);
            if (testvalue == null) Console.WriteLine($"NULL searching {key}");
            else Console.WriteLine($"{key} {testvalue[0]} {testvalue[1]}");

            int nte = 10000;

            sw.Restart();
            for (int i = 0; i < nte; i++)
            {
                int k = rnd.Next(nelements) + 1;
                object[] val = (object[])dn.Get(0, k); //(object[])dn.Portion(key % 4).Get(k);
                if (val == null)
                {
                    Console.WriteLine($"Found NULL key={k}");
                }
                //object[] val = (object[])dn.Portion(k % 4).Get(k);
            }
            sw.Stop();
            Console.WriteLine("get requests {0} for {1}", sw.ElapsedMilliseconds, nte);
        }
    }
    class DataNode 
    {
        private string path;
        //internal PType tp_element;
        internal FileOfBlocks fob;
        //private PType tp_list_element;
        private List<KVSequencePortion> sportion_list = new List<KVSequencePortion>();
        internal PaCell cell_list;
        internal PagedStream main_stream;
        private int N_streams_in_main { get { return (int)(main_stream.Length / PagedStream.HEAD_SIZE); } }
        public int NPortions { get { return sportion_list.Count; } }

        //private PType tp_common = null;
        private PaCell cell_common = null;
        private PType tp_configuration =
            new PTypeSequence(new PTypeRecord(
                new NamedType("tab_name", new PType(PTypeEnumeration.sstring)),
                new NamedType("key_type", PType.TType),
                new NamedType("value_type", PType.TType),
                new NamedType("key_grades", new PTypeSequence(new PTypeRecord(
                    new NamedType("portion_resource", new PType(PTypeEnumeration.integer))
                    )))
                ));
        private object common_data = null; // pobject в котором хранятся общие параметры узла и даже вся конфигурация
        private List<Tuple<string, object, object, List<Tuple<int>>>> config = null;
        public List<Tuple<string, object, object, List<Tuple<int>>>> Configuration { get
            {
                if (config == null) config = new List<Tuple<string, object, object, List<Tuple<int>>>>(
                    ((object[])((object[])common_data)[0]).Cast<object[]>().Select(co_objs => 
                        new Tuple<string, object, object, List<Tuple<int>>>(
                            (string)co_objs[0], co_objs[1], co_objs[2], 
                            ((object[])co_objs[3]).Cast<object[]>().Select(ob => new Tuple<int>((int)ob[0])).ToList<Tuple<int>>()
                        ))
                    );
                return config;
            } }
        private PType tp_common;
        private PType tp_list;
        public DataNode(string path)
        {
            this.path = path;
            //this.tp_element = tp_element;
            string dbpath = path + "DataNodeStorage.bin";
            bool fob_exists = File.Exists(dbpath);
            FileStream fs = new FileStream(dbpath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            fob = new FileOfBlocks(fs);
            Stream first_stream = fob.GetFirstAsStream();
            if (!fob_exists)
            {
                PagedStream.InitPagedStreamHead(first_stream, 8L, 0, PagedStream.HEAD_SIZE);
                fob.Flush();
            }
            main_stream = new PagedStream(fob, fob.GetFirstAsStream(), 8L);

            //long sz = PagedStream.HEAD_SIZE;

            // Если main_stream нулевой длины, надо инициировать конфигурацию стримов
            bool toinit = main_stream.Length == 0;
            if (toinit)
            {
                // инициируем 2 головы для потоков
                PagedStream.InitPagedStreamHead(main_stream, 0L, 0L, -1L);
                PagedStream.InitPagedStreamHead(main_stream, PagedStream.HEAD_SIZE, 0L, -1L);
                //PagedStream.InitPagedStreamHead(main_stream, 2 * sz, 0L, -1L);

                main_stream.Flush(); fob.Flush();
            }
            // создадим 2 потока
            Stream stream_common_params = new FileStream(path + "common.pac", FileMode.OpenOrCreate, FileAccess.ReadWrite); //new PagedStream(fob, main_stream, 0L);
            Stream stream_SP_list = new FileStream(path + "splist.pac", FileMode.OpenOrCreate, FileAccess.ReadWrite); //new PagedStream(fob, main_stream, 1 * PagedStream.HEAD_SIZE);
            //PagedStream stream_offsets = new PagedStream(fob, main_stream, 2 * sz); // пока не знаю для чего...

            // Создадим ячейки
            tp_common = new PTypeRecord(
                new NamedType("configuration", tp_configuration));
            cell_common = new PaCell(tp_common, stream_common_params, false);
            tp_list = new PTypeSequence(KVSequencePortion.tp_pobj);
            cell_list = new PaCell(tp_list, stream_SP_list, false);

            // Инициализируем 
            if (toinit)
            {
                cell_common.Fill(new object[] { new object[0] });
                cell_list.Fill(new object[0]);
            }
            common_data = cell_common.Root.Get();
            foreach (object[] p in cell_list.Root.ElementValues())
            {
                KVSequencePortion kvsp = new KVSequencePortion(this, p);
                sportion_list.Add(kvsp);
            }

        }
        public void ConfigNode(object commondata)
        {
            common_data = commondata;
            object conf = ((object[])common_data)[0];
            foreach (object[] tab in (object[])conf)
            {
                PType tp_tab_element = PType.FromPObject(tab[2]);
                object[] key_grades = (object[])tab[3];
                for (int i = 0; i < key_grades.Length; i++)
                {
                    var sportion = KVSequencePortion.Create(this, tp_tab_element);
                    cell_list.Root.AppendElement(sportion.pobj);
                    int iportion = sportion_list.Count;
                    sportion_list.Add(sportion);

                    key_grades[i] = new object[] { iportion };
                }
            }
            cell_list.Flush(); // Этот Flush нужен
            fob.Flush();
            object v = cell_list.Root.Get();

            cell_common.Clear();
            cell_common.Fill(common_data);
        }
        // Процедуры интерфеса
        public void Append(int itable, object[] pair, bool tododynamicindex)
        {
            int key = (int)pair[0];
            var list = this.Configuration;
            var grades = list[itable].Item4;
            var portion_info = grades[key % grades.Count];
            int iportion = portion_info.Item1;
            var portion = Portion(iportion);
            portion.AppendPair(pair, tododynamicindex);
        }
        public object Get(int itable, int key)
        {
            var list = this.Configuration;
            var grades = list[itable].Item4;
            var portion_info = grades[key % grades.Count];
            int iportion = portion_info.Item1;
            //int iportion = key % 4;
            var portion = Portion(iportion);
            return portion.Get(key);
        }

        // Создает в блочной памяти новый стрим и возвращает его offset в mainstream
        internal long CreateNewStreamInMain()
        {
            long offset = N_streams_in_main * PagedStream.HEAD_SIZE;
            PagedStream.InitPagedStreamHead(main_stream, offset, 0L, -1L); // Должен автоматически менять N_streams_in_main
            return offset;
        }

        public KVSequencePortion Portion(int i) { return sportion_list[i]; }
        public void Flush()
        {
            cell_common.Clear();
            cell_common.Fill(common_data);
            cell_common.Flush();

            cell_list.Flush();
            fob.Flush();
        }
    }
    class KVSequencePortion
    {
        private DataNode dn;
        internal static PType tp_pobj = new PTypeRecord(
            new NamedType("offset_for_seq", new PType(PTypeEnumeration.longinteger)),
            new NamedType("offset_for_keys", new PType(PTypeEnumeration.longinteger)),
            new NamedType("offset_for_offs", new PType(PTypeEnumeration.longinteger)),
            new NamedType("offset_for_dic1", new PType(PTypeEnumeration.longinteger)),
            new NamedType("offset_for_dic2", new PType(PTypeEnumeration.longinteger)),
            new NamedType("type_of_elements", PType.TType)
            );
        private long offset_for_seq;
        private long offset_for_keys;
        private long offset_for_offsets;
        private long offset_for_dic1;
        private long offset_for_dic2;
        private PType tp_element;

        internal object[] pobj { get { return new object[] { offset_for_seq, offset_for_keys, offset_for_offsets, offset_for_dic1, offset_for_dic2, tp_element.ToPObject(2) }; } }
        private PaCell keyvalue_seq;
        private PaCell keys;
        private PaCell offsets;
        private Dictionary<int, long> dic1 = new Dictionary<int, long>();
        private Dictionary<int, long> dic2 = new Dictionary<int, long>();
        private int[] keys_arr = null;
        private Func<int, Diapason> GetDia = null;
        internal KVSequencePortion(DataNode dn, 
            long offset_in_mainstream_for_seq, long offset_for_keys, long offset_for_offsets, long offset_for_dic1, long offset_for_dic2, PType tp_element)
        {
            this.dn = dn;
            this.offset_for_seq = offset_in_mainstream_for_seq;
            this.offset_for_keys = offset_for_keys;
            this.offset_for_offsets = offset_for_offsets;
            this.offset_for_dic1 = offset_for_dic1;
            this.offset_for_dic2 = offset_for_dic2;
            this.tp_element = tp_element;
            PType tp_sequ = new PTypeSequence(new PTypeRecord(
                new NamedType("key", new PType(PTypeEnumeration.integer)),
                new NamedType("value", tp_element)));
            // Создадим базу данных, состоящую из последовательности и двух индексных массивов: массива ключей и массива офсетов 
            PagedStream stream1 = new PagedStream(dn.fob, dn.main_stream, offset_in_mainstream_for_seq);
            keyvalue_seq = new PaCell(tp_sequ, stream1, false);
            PagedStream stream2 = new PagedStream(dn.fob, dn.main_stream, offset_for_keys);
            keys = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.integer)), stream2, false);
            //keys_arr = keys.IsEmpty ? new int[0] : keys.Root.ElementValues().Cast<int>().ToArray();
            PagedStream stream3 = new PagedStream(dn.fob, dn.main_stream, offset_for_offsets);
            offsets = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.longinteger)), stream3, false);
        }
        internal KVSequencePortion(DataNode dn, object[] p) : this(dn, (long)p[0], (long)p[1], (long)p[2], (long)p[3], (long)p[4], PType.FromPObject(p[5])) {  }
        internal static KVSequencePortion Create(DataNode dn, PType tp_el)
        {
            long offset1 = dn.CreateNewStreamInMain();
            long offset2 = dn.CreateNewStreamInMain();
            long offset3 = dn.CreateNewStreamInMain();
            return new KVSequencePortion(dn, offset1, offset2, offset3, 0L, 0L, tp_el);
        }
        public void Clear()
        {
            keyvalue_seq.Clear();
            keyvalue_seq.Fill(new object[0]);
            keys.Clear(); keys.Fill(new object[0]);
            offsets.Clear(); offsets.Fill(new object[0]);
            dic1 = new Dictionary<int, long>();
            dic2 = new Dictionary<int, long>();
        }
        public void Load(IEnumerable<object[]> keyvalueflow)
        {
            foreach (object[] keyvalue in keyvalueflow)
            {
                long offset = keyvalue_seq.Root.AppendElement(keyvalue);
                //dic1.Add((int)keyvalue[0], offset);
            }
            keyvalue_seq.Flush();
        }
        public void Activate()
        {
            if (keys_arr == null) keys_arr = keys.Root.ElementValues().Cast<int>().ToArray();
            GetDia = Program.GetDiaFunc3(keys_arr);
        }
        public IEnumerable<object[]> Pairs() { return keyvalue_seq.Root.ElementValues().Cast<object[]>(); }
        public long AppendPair(object[] pair, bool tododynamicindex)
        {
            long offset = keyvalue_seq.Root.AppendElement(pair);
            if (tododynamicindex) dic1.Add((int)pair[0], offset);
            return offset;
        }
        public void Flush()
        {
            keyvalue_seq.Flush();
            //dn.Flush(); 
        }
        public object Get(int search_key)
        {

            long offset;
            if (dic1 != null && dic1.Count > 0 && dic1.TryGetValue(search_key, out offset))
            {
                return ((object[])keyvalue_seq.Root.Element(0).SetOffset(offset).Get())[1];
            }
            if (dic2 != null && dic2.Count > 0 && dic2.TryGetValue(search_key, out offset))
            {
                return ((object[])keyvalue_seq.Root.Element(0).SetOffset(offset).Get())[1];
            }
            Diapason diap = GetDia(search_key);
            //int ifirst = 666888;
            int ifirst = Program.Get8FirstIndexOf((int)diap.start, (int)diap.numb, keys_arr, search_key);
            //int ifirst = Program.Get8FirstIndexOf(0, keys_arr.Length, keys_arr, search_key);
            if (ifirst == -1)
            {
                return null;
            }
            //entry.offset = offsets[ifirst]; // Использование массива 
            //entry.offset = (long)key_index.Root.Element(ifirst).Field(1).Get(); // Использование всего индекса  
            offset = (long)offsets.Root.Element(ifirst).Get();
            return ((object[])keyvalue_seq.Root.Element(0).SetOffset(offset).Get())[1];
        }
        public void CalculateDynamicIndex()
        {
            dic1 = new Dictionary<int, long>();
            keyvalue_seq.Root.Scan((off, obj) =>
            {
                int key = (int)((object[])obj)[0];
                dic1.Add(key, off);
                return true;
            });
        }
        public void CalculateStaticIndex()
        {
            dic1 = new Dictionary<int, long>(); // Не используем накопленное содержание
            dic2 = new Dictionary<int, long>();
            int ind = 0;
            int nelements = (int)keyvalue_seq.Root.Count();
            keys_arr = new int[nelements];
            long[] offs_arr = new long[nelements];
            keyvalue_seq.Root.Scan((off, obj) =>
            {
                int key = (int)((object[])obj)[0];
                keys_arr[ind] = key;
                offs_arr[ind] = off;
                ind++;
                return true;
            });
            Array.Sort(keys_arr, offs_arr);
            keys.Clear(); keys.Fill(new object[0]);
            List<int> keys_list = new List<int>();
            offsets.Clear(); offsets.Fill(new object[0]);
            // Будем убирать повторы
            int prev_key = Int32.MaxValue;
            long prev_offset = Int64.MinValue;
            for (int i=0; i<nelements; i++)
            {
                int key = keys_arr[i];
                long offset = offs_arr[i];
                if (key != prev_key)
                {  // Надо сохранить пару, но только если предыдущий ключ не фиктивный
                    if (prev_key != Int32.MaxValue)
                    {
                        keys.Root.AppendElement(prev_key);
                        keys_list.Add(prev_key);
                        offsets.Root.AppendElement(prev_offset);
                    }
                    prev_key = key;
                    prev_offset = offset;
                }
                else
                {
                    if (offset > prev_offset) prev_offset = offset;
                }
            }
            keys.Root.AppendElement(prev_key);
            keys_list.Add(prev_key);
            offsets.Root.AppendElement(prev_offset);

            // Доделаем массив ключей
            keys_arr = keys_list.ToArray();
            keys.Flush();
            offsets.Flush();
            //dn.Flush();
        }
    }
}
