using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    partial class Program
    {
        private static System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        public static void Main10()
        {
            Console.WriteLine("Start GetStarted/Main10");
            using (Stream tab_stream = File.Open(path + "tab_stream.bin", FileMode.OpenOrCreate))
            using (Stream spo_stream = File.Open(path + "spo_stream.bin", FileMode.OpenOrCreate))
            {
                Mag_Triple_Store store = new Mag_Triple_Store(tab_stream, spo_stream, null);

                int ntiples = 1_000_000;
                // Начало таблицы имен 0 - type, 1 - name, 2 - person
                int b = 3; // Начальный индекс назначаемых идентификаторов сущностей

                sw.Restart();
                var query = Enumerable.Range(0, ntiples)
                    .SelectMany(i => new object[]
                    {
                    new object[] { ntiples + b - i - 1, 0, new object[] { 1, 2 } },
                    new object[] { ntiples + b - i - 1, 1, new object[] { 2, "pupkin" + (ntiples + b - i - 1) } }
                    }); // по 2 триплета на запись
                store.Load(query);
                store.Build();
                sw.Stop();
                Console.WriteLine($"load of {ntiples * 2} triples ok. Duration={sw.ElapsedMilliseconds}");
                store.Look();
            }
        }
    }

    // =================== библлиотека =====================

    // Хранилище
    public class Mag_Triple_Store
    {
        // Есть таблица имен для хранения строк IRI
        //private Mag_Nametable nametable; // пока не используется

        // Тип Object Variants
        PType tp_ov = new PTypeUnion(
            new NamedType("dummy", new PType(PTypeEnumeration.none)),
            new NamedType("iri", new PType(PTypeEnumeration.integer)),
            new NamedType("str", new PType(PTypeEnumeration.sstring)));
        // Тип триплепта
        PType tp_triple;
        
        // Основная таблица - таблица триплетов
        private UniversalSequenceBase table;
        // Индекс
        private UniversalSequenceComp index_spo;
        // Компаратор
        Comparer<object> spo_comparer;
        // Конструктор
        public Mag_Triple_Store(Stream tab_stream, Stream spo_stream, Comparer<object> comp)
        {
            this.spo_comparer = comp;
            tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            table = new UniversalSequenceBase(tp_triple, tab_stream);
            spo_comparer = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                object[] aa = (object[])a; object[] bb = (object[])b;
                int cmp = ((int)aa[0]).CompareTo((int)bb[0]);
                return cmp;
            }));
            index_spo = new UniversalSequenceComp(new PType(PTypeEnumeration.longinteger), spo_stream, spo_comparer, table);
        }

        public void Load(IEnumerable<object> triples)
        {
            table.Clear();
            foreach (object tri in triples)
            {
                long off = table.AppendElement(tri);
                index_spo.AppendElement(off);
            }
            table.Flush();
            index_spo.Flush();
        }
        public void Build()
        {
            int nelements = (int)table.Count();
            object[] arr_triples = new object[nelements];
            long[] arr_offs = new long[nelements];
            for (int i = 0; i < nelements; i++)
            {
                long off = i == 0 ? 8L : table.ElementOffset();
                arr_offs[i] = off;
                object v = table.GetElement(off);
                arr_triples[i] = v;
            }
            Array.Sort(arr_triples, arr_offs, spo_comparer);
            index_spo.Clear();
            for (int i = 0; i < nelements; i++) index_spo.AppendElement(arr_offs[i]);
            index_spo.Flush();
        }
        public void Look()
        {
            foreach (long offset in index_spo.ElementValues().Take(10))
            {
                var v = table.GetElement(offset);
                Console.Write($"{tp_triple.Interpret(v)} ");
            }
            Console.WriteLine();
            int nprobe = 1000;
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int subj = rnd.Next((int)(index_spo.Count() / 2));
                long offset = index_spo.BinarySearchOffsetAny(new object[] { subj, null, null });
                if (offset == long.MinValue) throw new Exception("not found");
                var v = table.GetElement(offset);
                //Console.WriteLine($"subj={subj} triple={tp_triple.Interpret(v)}");
                
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetAny search ok. duration={sw.ElapsedMilliseconds}");

            sw.Restart();
            for (int i=0; i<nprobe; i++)
            {
                int subj = rnd.Next((int)(index_spo.Count() / 2));
                var res = index_spo.BinarySearchAllInside(0, index_spo.Count(), new object[] { subj, null, null })
                    .Select(off => table.GetElement(off));
                if (res.Count() != 2)
                {
                    var arr = res.ToArray();
                    foreach (object ob in res)
                    {
                        Console.WriteLine($"ok. {tp_triple.Interpret(ob)}");
                    }
                    Console.WriteLine();
                    var offs = index_spo.BinarySearchAllInside(0, index_spo.Count(), new object[] { subj, null, null }).ToArray();
                    foreach (var off in offs)
                    {

                    }
                }
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetAll search ok. duration={sw.ElapsedMilliseconds}");
        }
        public IEnumerable<object> GetTriplesBySubj(int subj)
        {
            object sample = new object[] { subj, null, null };
            //index_spo;
            return Enumerable.Empty<object>();
        }
    }



}
