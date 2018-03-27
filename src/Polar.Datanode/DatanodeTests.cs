using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Datanode
{
    public partial class Datanode
    {
        // =============== Общие решения ================
        private bool tobuild = false;
        private ConfigObject conf_demo = new ConfigObject();
        //private ConfigObject c;
        private void SetConfiguration(object[] conf)
        {
            configuration = conf;
            int ntabs = ((object[])conf[0]).Length;
            element_types = new PType[ntabs];
            tass = new TableSignatures[ntabs];
            for (int t = 0; t < ntabs; t++)
            {
                PType tp_el = PType.FromPObject(((object[])((object[])conf[0])[t])[1]);
                element_types[t] = tp_el;
                tass[t] = new TableSignatures(tp_el);
            }
            indicomm.SetTass(tass);
            if (ismaster)
            { // Послать приказ другим
                int nnodes = ((object[])conf[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    indicomm.Order(nd - 1, _setconfiguration, 0, conf); // нулевая таблица несущественна
                }
            }
        }
        public void InitNodes(int nn)
        {
            InitFunctions(); // Пока помещу сюда. В дальнейшем, можно будет организовать отдельную команду
            nodecode = nn;
            if (ismaster)
            {
                // Здесь надо инициировать другие узлы...
                int nnodes = ((object[])configuration[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    // Номер канала (в узле мастера) на единицу меньше номера узла
                    indicomm.Order(nd - 1, _initcommunications, 0, nd); // указывается нулевая таблица
                }
            }
        }
        public void CreateDatabase()
        {
            if (sstorage != null) sstorage.Close();
            //char last = path[path.Length - 1];
            //if (last != '/' && last != '\\') path = path + "/";
            string filename = path + "streamstorage.bin";
            bool exists = System.IO.File.Exists(filename);
            if (exists) System.IO.File.Delete(filename);
            sstorage = new Polar.PagedStreams.StreamStorage(filename);
            portions = new List<KVSequencePortion>();

            object[] parts = ((object[])(configuration[2]))
                .Cast<object[]>()
                .Where(sec => (int)sec[2] == nodecode)
                .ToArray();
            sections = new List<Secrecord>();
            List<int[]> portion_streams_list = new List<int[]>();
            foreach (object[] part in parts)
            {
                int n1, n2, n3;
                sstorage.CreateStream(out n1);
                sstorage.CreateStream(out n2);
                sstorage.CreateStream(out n3);

                List<object> stream_numbers = new List<object>(new object[] { n1, n2, n3 });

                int tab = (int)part[0];
                int[] index_columns = tab2indxs(tab);

                foreach (int col in index_columns)
                {
                    int n4, n5;
                    sstorage.CreateStream(out n4);
                    sstorage.CreateStream(out n5);
                    stream_numbers.Add(n4);
                    stream_numbers.Add(n5);
                }

                portion_streams_list.Add(stream_numbers.Select(ob => (int)ob).ToArray());

                object[] por_numbers = stream_numbers.ToArray();
                //part[3] = por_numbers; // прямое размещение номеров в конфигураторе для мастера, но слэйвы должны эти конфигуратор вернуть 
                PType tp_element = tab2t(tab);
                var portion = new KVSequencePortion(sstorage, tp_element, por_numbers);
                int ind_portion = portions.Count;
                portions.Add(portion);
                //TODO: portions.Add(new Tuple<int, int>((int)part[0], (int)part[1]), portion);
                sections.Add(new Secrecord() { table = (int)part[0], section = (int)part[1], portion = ind_portion });
            }

            //// Создадим portion_streams и запомним
            //int[][] portion_streams = portion_streams_list.ToArray();
            //PaCell porstreamscell = new PaCell(new PTypeSequence(new PTypeSequence(new PType(PTypeEnumeration.integer))),
            //    path + "porstreams.pac", false);
            //porstreamscell.Clear();
            //porstreamscell.Fill(portion_streams.Select(arr => arr.Select(i => (object)i).ToArray()).ToArray());
            //porstreamscell.Close();

            // Послать команду другим узлам
            if (ismaster)
            {
                // Пригодится функция (t p n) => part в конфигураторе

                Func<int, int, int, object[]> tpn2part = (t, p, n) =>
                    ((object[])configuration[2])
                        .Cast<object[]>()
                        .First(sec => (int)sec[0] == t && (int)sec[1] == p && (int)sec[2] == n);
                // Здесь надо инициировать другие узлы...
                int nnodes = ((object[])configuration[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    // Номер канала (в узле мастера) на единицу меньше номера узла
                    object[] prts = (object[])indicomm.Order(nd - 1, _createdatabase, 0, null); // указывается нулевая таблица
                }
            }
        }


        public void StartToConfigure()
        {
            if (!ismaster) throw new Exception("Err: 39348");
            tobuild = true;
            conf_demo.DropTables(indicomm.NChannels + 1);
        }

        public int CreateTable(string name, PType tp_el, int[] exkey_columns)
        {
            if (!ismaster || !tobuild) throw new Exception("Err: 293456");
            return conf_demo.CreateTable(name, tp_el, exkey_columns);
        }
        public int CreateTable(string name, PType tp_el) { return CreateTable(name, tp_el, new int[0]); }


        // Процедура добавления записи в таблицу с добавление внешних индексов. Годится только для мастера!
        public void AppendRecord(int tab, object[] pair, bool dynindex)
        {
            if (!ismaster) throw new Exception("Assert err: 239445");
            // Обработаем основную последовательность
            AppendOnlyRecord(tab, pair, dynindex);
            //AppendPair(tab, pair);
            // Обработаем внешние индексы, если есть
            var index_columns = tab2indxs(tab);
            int i = 0;
            foreach (var indx_nom in index_columns)
            {
                int id = (int)pair[0];
                int ext_key = (int)pair[indx_nom];
                AppendOnlyExtKey(tab, i, ext_key, id, dynindex);
                i++;
            }
        }

        private struct AOEK { internal Datanode dn; internal int tab, i, ext_key, id; internal bool dynindex; };
        BufferredProcessing<AOEK>[] aoek_buffers = null;

        // Буферизованная процедура добавления записи в таблицу с добавление внешних индексов. Годится только для мастера!
        public void AppendRecord2(int tab, object[] pair, bool dynindex)
        {
            if (!ismaster) throw new Exception("Assert err: 239445");
            // Обработаем основную последовательность
            AppendOnlyRecord(tab, pair, dynindex);
            //AppendPair(tab, pair);
            // Обработаем внешние индексы, если есть
            var index_columns = tab2indxs(tab);
            int i = 0;
            foreach (var indx_nom in index_columns)
            {
                int id = (int)pair[0];
                int ext_key = (int)pair[indx_nom];
                //AppendOnlyExtKey(tab, i, ext_key, id, dynindex);
                int ext_lay = ext_key % tab2n(tab);
                aoek_buffers[ext_lay].Add(new AOEK() { dn = this, tab = tab, i = i, ext_key = ext_key, id = id, dynindex = dynindex });
                i++;
            }
        }

        private void AppendOnlyExtKey(int tab, int indx_nom, int ext_key, int pri_key, bool dynindex)
        {
            int ext_lay = ext_key % tab2n(tab);
            int ex_nd = tablay2node(tab, ext_lay);
            if (ex_nd == nodecode)
            {
                int ind_portion = sections.First(s => s.table == tab && s.section == ext_lay).portion;
                var porti = portions[ind_portion];
                porti.AppendExtKey(indx_nom, ext_key, pri_key, dynindex);
            }
            else
            {
                indicomm.Order(ex_nd - 1, _appendonlyextkey, tab, new object[] { indx_nom, ext_key, pri_key, dynindex });
            }
        }


        private void AppendOnlyRecord(int tab, object[] pair, bool dynindex)
        {
            int id = (int)pair[0];
            int lay = id % tab2n(tab);
            int nde = tablay2node(tab, lay);
            if (nde == nodecode)
            { // Этот узел (иногда мастер, иногда нет...)
                //int ind_portion = tabsec2por(tab, lay);
                int ind_portion = sections.First(s => s.table == tab && s.section == lay).portion;
                var porti = portions[ind_portion];
                porti.AppendPair(pair, dynindex);
            }
            else
            {
                indicomm.Order(nde - 1, _appendonlyrecord, tab, new object[] { pair, dynindex }); // надо сократить
            }
        }
        public void Flush()
        {
            foreach (var porti in portions)
            {
                porti.Flush();
            }
            if (ismaster)
            {
                // Здесь надо инициировать другие узлы...
                int nnodes = ((object[])configuration[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    indicomm.Order(nd - 1, _flush, 0, null); // указывается нулевая таблица
                }
            }
        }
        public void CalculateStaticIndex()
        {
            foreach (var porti in portions)
            {
                porti.CalculateStaticIndex();
            }
            if (ismaster)
            {
                // Здесь надо инициировать другие узлы...
                int nnodes = ((object[])configuration[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    indicomm.Order(nd - 1, _calculatestaticindex, 0, null); // указывается нулевая таблица
                }
            }
        }
        public void Activate()
        {
            foreach (var porti in portions)
            {
                porti.Activate();
            }
            if (ismaster)
            {
                // Здесь надо инициировать другие узлы...
                int nnodes = ((object[])configuration[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    indicomm.Order(nd - 1, _activate, 0, null); // таблица не существена
                }
            }
        }
        private object GetByKey(int tab, int key)
        {
            object v = null;
            int lay = key % tab2n(tab);
            int node = tablay2node(tab, lay);
            KVSequencePortion portion;
            if (node == nodecode)
            { // Этот узел (мастер)
                int ind_portion = sections.Where(s => s.table == tab && s.section == lay).First().portion;
                portion = portions[ind_portion];
                v = portion.Get(key);
            }
            else
            {
                if (!ismaster) throw new Exception("Err: 218237");
                v = indicomm.Order(node - 1, _getbykey, tab, new object[] { key }); // надо сократить
            }
            return v;
        }
        private object[] GetAllPrimaryByExternal(int tab, int exindnom, int exkey)
        {
            object[] v = null;
            int node = tablay2node(tab, exkey % tab2n(tab));
            KVSequencePortion portion;
            if (node == nodecode)
            { // Этот узел (мастер)
                int ind_portion = sections.Where(s => s.table == tab && s.section == (exkey % tab2n(tab))).First().portion;
                portion = portions[ind_portion];
                v = portion.GetAllPrimaryByExternal(exindnom, exkey).Cast<object>().ToArray();
            }
            else
            {
                if (!ismaster) throw new Exception("Err: 218237");
                v = (object[])indicomm.Order(node - 1, _getallprimarybyexternal, tab, new object[] { exindnom, exkey }); // надо сократить
            }
            return v;
        }


        public void FinishToConfigure()
        {
            conf_demo.FillSectionConf();
            SetConfiguration(conf_demo.Value);
            InitNodes(0);
            CreateDatabase();
            SaveConfiguration(null);
        }

        //public void ConnectToDatabase()
        //{
        //    LoadConfiguration();
        //    SetConfiguration(configuration); // Подозрительный момент - переписывание конфигуратора из себя в себя...
        //    InitNodes(0);
        //    ActivateDatabase();
        //    Activate();
        //}


        public void Test3(int nelements) // В оригинале Demo1
        {
            if (!ismaster) return;
            Console.WriteLine($"Start Demo1({nelements}). time in milliseconds.");

            Console.Write("Creating 3 tables... ");
            tobuild = true;
            conf_demo.DropTables(indicomm.NChannels + 1);
            int tab_persons = CreateTable("persons", new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer))));
            int tab_photos = CreateTable("photos", new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring))));
            int tab_reflections = CreateTable("reflections", new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("reflected", new PType(PTypeEnumeration.integer)),
                new NamedType("indoc", new PType(PTypeEnumeration.integer))
                ), new int[] { 1, 2 });

            //conf_demo.FillSectionConf();
            conf_demo.CreateSections("persons", new int[] { 10 });
            conf_demo.CreateSections("photos", new int[] { 20 });
            conf_demo.CreateSections("reflections", new int[] { 30 });

            SetConfiguration(conf_demo.Value);
            InitNodes(0);
            CreateDatabase();
            SaveConfiguration(null);

            Console.WriteLine("ok.");

            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            Console.Write($"Loading data for {nelements * 25} cells in 3 tables... ");
            sw.Restart();

            int npersons = nelements;
            bool dynindex = false;

            var query1 = Enumerable.Range(0, npersons)
                .Select(i => new object[] { npersons - 1 - i, "Pupkin" + (npersons - 1 - i) + "_" + rnd.Next(npersons), 20 + rnd.Next(80) });
            foreach (var rec in query1) AppendRecord(0, rec, dynindex);

            int nphotos = 2 * nelements;
            var query2 = Enumerable.Range(0, nphotos)
                .Select(i => new object[] { nphotos - 1 - i, "DSP_" + (nphotos - 1 - i) });
            foreach (var rec in query2) AppendRecord(1, rec, dynindex);

            int nreflections = 6 * nelements;
            var query3 = Enumerable.Range(0, nreflections)
                .Select(i => new object[] { nreflections - 1 - i, rnd.Next(npersons - 1), rnd.Next(nphotos - 1) });
            // Работа через буфера (таблица 2):
            int nsecs = tab2n(2);
            aoek_buffers = Enumerable.Repeat<BufferredProcessing<AOEK>>(new BufferredProcessing<AOEK>(10000, flow =>
            {
                foreach (var prs in flow)
                {
                    prs.dn.AppendOnlyExtKey(prs.tab, prs.i, prs.ext_key, prs.id, prs.dynindex);
                }
                //Console.WriteLine($"{flow.Count()} processed");
            }), nsecs).ToArray();
            // буду пользоваться AppendRecord2 в котором задействован буфер
            foreach (var rec in query3) AppendRecord2(2, rec, dynindex);
            // Еще сброс буферов
            foreach (var buf in aoek_buffers) buf.Flush();

            Flush();
            sw.Stop();
            Console.WriteLine($"ok. duration={sw.ElapsedMilliseconds}");

            Console.Write($"Calculating static index... ");
            sw.Restart();
            CalculateStaticIndex();
            sw.Stop();
            Console.WriteLine($"ok. duration={sw.ElapsedMilliseconds}");
            tobuild = false;

            Activate();

            //// ======================
            //if (indicomm.NChannels == 0) sstorage.LoadCache();
            //// ======================

            int nte = 100000;
            Console.Write($"GetByKey(k) test for persons table... ");
            sw.Restart();
            for (int i = 0; i < nte; i++)
            {
                int k = rnd.Next(nelements);
                object[] val = (object[])GetByKey(tab_persons, k);
                if (val == null) { Console.WriteLine($"Found NULL key={k}"); }
            }
            sw.Stop();
            Console.WriteLine($"ok. duration {sw.ElapsedMilliseconds} for {nte} random keys");

            //IEnumerable<object> GetReflectionsByReflected(int rkey)
            //{
            //    object[] vv = GetAllPrimaryByExternal(2, 0, rkey);
            //    foreach (int k in vv)
            //    {
            //        object[] rec = (object[])GetByKey(2, k);
            //        int k2 = (int)rec[2];
            //        yield return GetByKey(1, k2);
            //    }
            //}
            Func<int, IEnumerable<object>> GetReflectionsByReflected = rkey => GetAllPrimaryByExternal(2, 0, rkey)
                .Cast<int>().Select(k => GetByKey(1, (int)((object[])GetByKey(2, k))[2]));
            nte = 1000;
            sw.Restart();
            long sum = 0;

            Console.Write($"Portrait(k) test for persons... ");
            for (int i = 0; i < nte; i++)
            {
                int id = rnd.Next(0, nelements);
                var qu = GetReflectionsByReflected(id).ToArray();
                sum += qu.Count();
            }
            sw.Stop();
            Console.WriteLine($"ok. duration {sw.ElapsedMilliseconds} for {nte} random keys. sum={sum}");

        }
    }
}
