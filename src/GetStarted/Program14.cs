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
            string path = "../../../";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start GetStarted/Main14");
            int cnt = 0;
            TripleStoreInt32 store = new TripleStoreInt32(() => new FileStream(path + "Databases/f" + (cnt++) + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite));
            int nelements = 50_000_000;
            // Начало таблицы имен 0 - type, 1 - name, 2 - person
            int b = 3; // Начальный индекс назначаемых идентификаторов сущностей

            bool toload = true;
            if (toload)
            {
                sw.Restart();
                var query = Enumerable.Range(0, nelements)
                    .SelectMany(i => new object[]
                    {
                    new object[] { nelements + b - i - 1, 0, new object[] { 1, 2 } },
                    new object[] { nelements + b - i - 1, 1, new object[] { 2, "" + (nelements + b - i - 1) } }
                    }); // по 2 триплета на запись
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
            //var qu = store.GetBySubj(ke);
            //foreach (object[] t in qu)
            //{
            //    Console.WriteLine($"{t[0]} {t[1]}");
            //}

            var qu2 = store.GetByObjString("p12345");
            foreach (object[] t in qu2)
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

            nprobe = 1000;
            sw.Restart();
            int total = 0;
            for (int i = 0; i < nprobe; i++)
            {
                var quer = store.GetByObjString("" + rnd.Next(nelements));
                total += quer.Count();
            }
            sw.Stop();
            Console.WriteLine($"Test === OBJECT===== {nprobe} queries for {total} elements. duration={sw.ElapsedMilliseconds}");
        }

    }
    public class TripleStoreInt32
    {
        private UniversalSequenceBase table;
        private IndexKey32CompImm s_index;
        private IndexKey32CompImm o_index;
        public static Func<object, int> Test_keyfun = null; 
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
            string selected_chars = "!\"#$%&\'()*+,-./0123456789:;<=>?@abcdefghjklmnopqrstuwxyz{|}~абвгдежзийклмнопрстуфхцчшщъыьэюяё";
            Func<object, int> halfKeyFun = ob =>
            {
                object[] tri = (object[])ob;
                object[] pair = (object[])tri[2];
                int tg = (int)pair[0];
                if (tg == 1) // iri
                {
                    return 1 - (int)pair[1];
                }
                else if (tg == 2) // str
                {
                    string s = (string)pair[1];
                    int len = s.Length;
                    var chs = s.ToCharArray()
                        .Concat(Enumerable.Repeat(' ', len < 4 ? 4 - len : 0))
                        .Take(4)
                        .Select(ch =>
                        {
                            int ind = selected_chars.IndexOf(ch);
                            if (ind == -1) ind = 0; // неизвестный символ помечается как '!'
                            return ind;
                        }).ToArray();
                    return (chs[0] << 24) | (chs[1] << 16) | (chs[2] << 8) | chs[3]; 
                }
                throw new Exception("Err: 292333");
            };
            Test_keyfun = halfKeyFun;
            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                object[] aa = (object[])((object[])a)[2];
                object[] bb = (object[])((object[])b)[2];
                int a1 = (int)aa[0];
                int b1 = (int)bb[0];
                int cmp = a1.CompareTo(b1);
                if (cmp != 0) return cmp;
                if (a1 == 1) return ((int)aa[1]).CompareTo(((int)bb[1]));
                return ((string)aa[1]).CompareTo(((string)bb[1]));
            }));

            o_index = new IndexKey32CompImm(stream_gen, table, halfKeyFun, comp);
        }
        public void Build(IEnumerable<object> triples)
        {
            Load(triples);
            s_index.Build();
            o_index.Build();
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
            o_index.Refresh();
        }
        public IEnumerable<object> GetBySubj(int subj)
        {
            //return s_index.GetAllByKeyRAM(subj);
            return s_index.GetAllBySample(new object[] { subj, -1, null });
        }
        public IEnumerable<object> GetByObjString(string s)
        {
            return o_index.GetAllBySample(new object[] { -1, -1, new object[] { 2, s } });
        }

    }
}