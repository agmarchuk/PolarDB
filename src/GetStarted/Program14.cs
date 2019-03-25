using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    partial class Program
    {
        public static void Main14()
        {
            string path = "";// "../../../";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start GetStarted/Main14");
            int cnt = 0;
            TripleStoreInt32 store = new TripleStoreInt32(() => new FileStream(path + "Databases/f" + (cnt++) + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite));
            int nelements = 5_000_000;
            // Начало таблицы имен 0 - type, 1 - name, 2 - person
            int b = 3; // Начальный индекс назначаемых идентификаторов сущностей

            var query = Enumerable.Range(0, nelements)
                .SelectMany(i => new object[]
                {
                    new object[] { nelements + b - i - 1, 0, new object[] { 1, 2 } },
                    new object[] { nelements + b - i - 1, 1, new object[] { 2, "pupkin" + (nelements + b - i - 1) } }
                }); // по 2 триплета на запись

            bool toload = false;
            if (toload)
            {
                sw.Restart();
                store.Build(query);
                sw.Stop();
                Console.WriteLine($"load of {nelements * 2} triples ok. Duration={sw.ElapsedMilliseconds}");

            }
            else
            {
                sw.Restart();
                store.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh for {nelements * 2} triples ok. Duration={sw.ElapsedMilliseconds}");
            }

            // Для проверки работы запрошу запись с ключом nelements * 2 / 3
            int ke = nelements * 2 / 3 + 2;
            var qu = store.GetBySubj(ke);
            foreach (object[] t in qu)
            {
                Console.WriteLine($"{t[0]} {t[1]}");
            }

            int nprobe = 1000;
            Random rnd = new Random();

            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int subj = rnd.Next(nelements);
                var quer = store.GetBySubj(subj);
                if (quer.Count() != 2)
                {
                    foreach (object[] t in quer)
                    {
                        Console.WriteLine($"{t[0]} {t[1]}");
                    }
                }
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetAll search ok. duration={sw.ElapsedMilliseconds}");

            /*


            nprobe = 10000;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int subj = rnd.Next(nelements);
                var quer = store.GetTest(subj);
                if (quer.Count() != 2)
                {
                    foreach (object[] t in quer)
                    {
                        Console.WriteLine($"{t[0]} {t[1]}");
                    }
                }
            }
            sw.Stop();
            Console.WriteLine($"Test ========= {nprobe} GetAll search ok. duration={sw.ElapsedMilliseconds}");
            */
        }

    }
    public class TripleStoreInt32
    {
        private UniversalSequenceBase table;
        private IndexKey32CompImm s_index;
        public TripleStoreInt32(Func<Stream> stream_gen)
        {
            // Тип Object Variants
            PType tp_ov = new PTypeUnion(
                new NamedType("dummy", new PType(PTypeEnumeration.none)),
                new NamedType("iri", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)));
            PType tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            table = new UniversalSequenceBase(tp_triple, stream_gen());
            s_index = new IndexKey32CompImm(stream_gen, table, ob => (int)((object[])ob)[0], null); 
        }
        public void Build(IEnumerable<object> triples)
        {
            Load(triples);
            s_index.Build();
        }
        private void Load(IEnumerable<object> triples)
        {
            table.Clear();
            foreach (object tri in triples)
            {
                long off = table.AppendElement(tri);
            }
            table.Flush();
        }
        public void Refresh()
        {
            s_index.Refresh();
            table.Refresh();
        }
        public IEnumerable<object> GetBySubj(int subj)
        {
            return s_index.GetBySubj(subj);
        }

    }
}