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
        public static void Main16()
        {
            string path = "../../../";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Random rnd = new Random();
            Console.WriteLine("Start GetStarted/Main16");
            int cnt = 0;
            Func<Stream> GenStream = () => new FileStream(path + "Databases/f" + (cnt++) + ".bin", 
                FileMode.OpenOrCreate, FileAccess.ReadWrite);

            TripleStoreInt32 store = new TripleStoreInt32(GenStream);

            int npersons = 4_000_000;
            bool toload = false;

            if (toload)
            {
                //nt.Clear();
                //nt.GetSetStr("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
                //int name_cod = nt.GetSetStr("http://fogid.net/o/name");

                IEnumerable<object> qu_persons = Enumerable.Range(0, npersons)
                    .SelectMany(i =>
                    {
                        return new object[] {
                                        new object[] { "p" + i, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                            new object[] { 1, "http://fogid.net/o/person"}},
                                        new object[] { "p" + i, "http://fogid.net/o/name",
                                            new object[] { 2, "p"+i}},
                                        new object[] { "p" + i, "http://fogid.net/o/age",
                                            new object[] { 2, "33 years"}}
                        };
                    });
                IEnumerable<object> qu_fotos = Enumerable.Range(0, npersons * 2)
                    .SelectMany(i =>
                    {
                        return new object[] {
                                        new object[] { "f" + i, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                            new object[] { 1, "http://fogid.net/o/photo"}},
                                        new object[] { "f" + i, "http://fogid.net/o/name",
                                            new object[] { 2, "DSP"+i}}
                        };
                    });
                IEnumerable<object> qu_reflections = Enumerable.Range(0, npersons * 6)
                    .SelectMany(i =>
                    {
                        return new object[] {
                                        new object[] { "r" + i, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                            new object[] { 1, "http://fogid.net/o/reflection"}},
                                        new object[] { "r" + i, "http://fogid.net/o/reflected",
                                            new object[] { 1, "p"+rnd.Next(npersons)}},
                                        new object[] { "r" + i, "http://fogid.net/o/indoc",
                                            new object[] { 1, "f"+rnd.Next(npersons*2)}}
                        };
                    });

                sw.Restart();
                var ntriples = store.GenerateTripleFlow(qu_persons.Concat(qu_fotos).Concat(qu_reflections));
                foreach (var t in ntriples.Skip(98).Take(10)) Console.WriteLine(store.ToStr(t));
                store.Build(ntriples);
                sw.Stop();
                Console.WriteLine($"store Build ok. Duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                sw.Restart();
                store.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh for Phototeka {npersons} persons. Duration={sw.ElapsedMilliseconds}");
            }


            // Испытываю
            // Беру персону 
            string id = "p" + (npersons * 2 / 3);
            int nid, reflected, indoc, name;
            if (!store.TryGetCode(id, out nid)) throw new Exception("338433");
            if (!store.TryGetCode("http://fogid.net/o/reflected", out reflected)) throw new Exception("338434");
            if (!store.TryGetCode("http://fogid.net/o/indoc", out indoc)) throw new Exception("338435");
            if (!store.TryGetCode("http://fogid.net/o/name", out name)) throw new Exception("338436");
            var query1 = store.GetInverse(nid)
                .Cast<object[]>()
                .Where(t => (int)t[1] == reflected)
                .SelectMany(t => store.GetBySubj((int)t[0])
                    .Where(tt => (int)((object[])tt)[1] == indoc)
                    .Select(tt => store.GetBySubj((int)((object[])((object[])tt)[2])[1])
                        .Where(ttt => (int)((object[])ttt)[1] == name)
                        .FirstOrDefault()       )
                    )
                    
                ;
            foreach (object[] t in query1)
            {
                Console.WriteLine(store.DecodeTriple(t));
            }
            Console.WriteLine();


            int nprobe = 1000;
            int total = 0;
            sw.Restart();
            for (int i=0; i<nprobe; i++)
            {
                // Беру случайную персону
                if (!store.TryGetCode("p" + rnd.Next(npersons), out nid)) throw new Exception("338437");
                var fots = store.GetInverse(nid)
                    .Cast<object[]>()
                    .Where(t => (int)t[1] == reflected)
                    .SelectMany(t => store.GetBySubj((int)t[0])
                        .Where(tt => (int)((object[])tt)[1] == indoc)
                        .Select(tt => store.GetBySubj((int)((object[])((object[])tt)[2])[1])
                            .Where(ttt => (int)((object[])ttt)[1] == name)
                            .FirstOrDefault())
                        ).ToArray();
                total += fots.Length;
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} tests ok. Duration={sw.ElapsedMilliseconds}");
            Console.WriteLine("========");

            var que = store.GetByObjName("p2").ToArray();
            foreach (var tri in que)
            {
                Console.WriteLine(store.ToStr(tri));
            }

            sw.Restart();
            total = 0;
            for (int i = 0; i < nprobe; i++)
            {
                string scod = "p" + rnd.Next(npersons);
                total += store.GetByObjName(scod).Where(obj => (string)((object[])((object[])obj)[2])[1] == scod).Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetByObjString tests ok. Duration={sw.ElapsedMilliseconds} total={total}");
            Console.WriteLine();


            return;
            // конец проверки

            int nelements = 5_000_000;
            // Начало таблицы имен 0 - type, 1 - name, 2 - person, 3 - parent
            int b = 4; // Начальный индекс назначаемых идентификаторов сущностей

            if (toload)
            {
                sw.Restart();
                var query = Enumerable.Range(0, nelements)
                    .SelectMany(i => new object[]
                    {
                    new object[] { nelements + b - i - 1, 0, new object[] { 1, 2 } },
                    new object[] { nelements + b - i - 1, 1, new object[] { 2, "" + (nelements + b - i - 1) } },
                    new object[] { nelements + b - i - 1, 3, new object[] { 1, rnd.Next(nelements) + b } } // родитель
                    }); // по 3 триплета на запись
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
            var qui = store.GetInverse(ke);
            foreach (object[] t in qui)
            {
                Console.WriteLine($"{t[0]} {t[1]} {((object[])t[2])[1]}");
            }

            nprobe = 1000;

            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int subj = rnd.Next(nelements);
                var quer = store.GetBySubj(subj);
                if (quer.Count() != 3)
                {
                    foreach (object[] t in quer)
                    {
                        Console.WriteLine($"{t[0]} {t[1]}");
                    }
                }
            }
            sw.Stop();
            Console.WriteLine($"{nelements} elements {nprobe} GetAll search ok. duration={sw.ElapsedMilliseconds}");

            sw.Restart();
            total = 0;
            for (int i = 0; i < nprobe; i++)
            {
                int obj = rnd.Next(nelements);
                var quer = store.GetInverse(obj);
                total += quer.Count();
            }
            sw.Stop();
            Console.WriteLine($"inverse elements {nprobe} GetInverse search ok. duration={sw.ElapsedMilliseconds} total = {total}");

            return;

            var qu2 = store.GetByObjName("p12345");
            foreach (object[] t in qu2)
            {
                Console.WriteLine($"{t[0]} {t[1]}");
            }


            nprobe = 1000;
            sw.Restart();
            total = 0;
            for (int i = 0; i < nprobe; i++)
            {
                var quer = store.GetByObjName("" + rnd.Next(nelements));
                total += quer.Count();
            }
            sw.Stop();
            Console.WriteLine($"Test === OBJECT===== {nprobe} queries for {total} elements. duration={sw.ElapsedMilliseconds}");

        }

    }

    /// <summary>
    /// Таблица имен с 32-разрядной базой кодирования и хеширования. Таблица сопоставляет строке целочисленный (32-разряда)
    /// код. Можно добавлять строки, можно по строке получать код, можно по коду получать строку.
    /// </summary>
    public class Nametable32
    {
        // Носителем таблицы является последовательность пар {код, строка}. Номер строки - ее код. Это первично. 
        // По коду строка определяется однозначно (как вводили), по строке код может определяться с учетом эквивалентностей.
        // Вначале таблица пустая, она заполняется 
        private UniversalSequenceBase table;
        private UniversalSequence<long> str_offsets;
        private IndexKey32CompImm name_index;
        private Dictionary<string, int> dyna_index;
        public Nametable32(Func<Stream> stream_gen)
        {
            PType tp_elem = new PTypeRecord(
                new NamedType("code", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)));
            table = new UniversalSequenceBase(tp_elem, stream_gen());
            str_offsets = new UniversalSequence<long>(new PType(PTypeEnumeration.longinteger), stream_gen());
            Comparer<object> comp_str = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                var aa = (string)((object[])a)[1];
                var bb = (string)((object[])b)[1];
                return aa.CompareTo(bb);
            }));

            name_index = new IndexKey32CompImm(stream_gen, table, 
                ob => Hashfunctions.HashRot13((string)((object[])ob)[1]), comp_str);
            dyna_index = new Dictionary<string, int>();
        }
        public void Clear()
        {
            table.Clear();
            str_offsets.Clear();
            name_index.Clear();
            dyna_index = new Dictionary<string, int>();
        }
        //public void Load(IEnumerable<string> flow)
        //{
        //    // а правильно ли без очистки name_index ?
        //    int cod = (int)table.Count();
        //    foreach (string s in flow)
        //    {
        //        long off = table.AppendElement(new object[] { cod, s });
        //        cod++;
        //        str_offsets.AppendElement(off);
        //    }
        //    table.Flush();
        //    str_offsets.Flush();
        //}
        public void Build()
        {
            // Про порядок операторов еще надо подумать
            dyna_index = new Dictionary<string, int>();
            name_index.Build();
        }
        public void Refresh()
        {
            table.Refresh();
            str_offsets.Refresh();
            name_index.Refresh();
        }
        // ==================== Динамика ===================
        private int SetStr(string s)
        {
            int code = (int)table.Count();
            long off = table.AppendElement(new object[] { code, s });
            str_offsets.AppendElement(off);
            dyna_index.Add(s, code);
            // нужен итоговый Flush по двум последовательностям
            return code;
        }
        public void Flush()
        {
            table.Flush();
            str_offsets.Flush();
        }
        public bool TryGetCode(string s, out int code)
        {
            if (dyna_index.TryGetValue(s, out code)) return true;
            var q = name_index.GetAllBySample(new object[] { -1, s }).FirstOrDefault(ob => (string)((object[])ob)[1] == s);
            if (q == null) return false;
            code = (int)((object[])q)[0];
            return true;
        }
        public int GetSetStr(string s)
        {
            int code;
            if (TryGetCode(s, out code)) return code;
            code = SetStr(s);
            return code;
        }
        public string Decode(int cod)
        {
            long off = (long)str_offsets.GetByIndex(cod);
            return (string)((object[])table.GetElement(off))[1];
        }


    }

    public class TripleStoreInt32
    {
        private Nametable32 nt;
        private UniversalSequenceBase table;
        private IndexKey32CompImmutable s_index;
        private IndexKey32Imm i_index;
        private IndexKey32CompImmutable name_index;
        public TripleStoreInt32(Func<Stream> stream_gen)
        {
            // сначала таблица имен
            nt = new Nametable32(stream_gen);
            // Тип Object Variants
            PType tp_ov = new PTypeUnion(
                new NamedType("dummy", new PType(PTypeEnumeration.none)),
                new NamedType("iri", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)),
                new NamedType("int", new PType(PTypeEnumeration.sstring)),
                new NamedType("date", new PType(PTypeEnumeration.sstring)),
                new NamedType("langstr", new PTypeRecord(
                    new NamedType("lang", new PType(PTypeEnumeration.sstring)),
                    new NamedType("str", new PType(PTypeEnumeration.sstring)))));
            PType tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            // Главная последовательность кодированных триплетов
            table = new UniversalSequenceBase(tp_triple, stream_gen());

            // прямой ссылочный индекс
            s_index = new IndexKey32CompImmutable(stream_gen, table, 
                ob => Enumerable.Repeat<int>((int)((object[])ob)[0], 1), null);
            
            // Обратный ссылочный индекс
            i_index = new IndexKey32Imm(stream_gen, table, obj =>
            {
                object[] pair = (object[])((object[])obj)[2];
                int tg = (int)pair[0];
                if (tg != 1) return Enumerable.Empty<int>();
                return Enumerable.Repeat<int>((int)pair[1], 1);
            }, null);

            // Индекс по тексту объектов триплетов с предикатом http://fogid.net/o/name
            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                return string.Compare((string)((object[])((object[])a)[2])[1], (string)((object[])((object[])b)[2])[1]);
            }));
            int name_code = Int32.MinValue; // nt.GetSetStr("http://fogid.net/o/name"); // Такое предварительное вычисление не работает!!!
            name_index = new IndexKey32CompImmutable(stream_gen, table, obj =>
                {
                    if (name_code == Int32.MinValue) name_code = nt.GetSetStr("http://fogid.net/o/name");
                    object[] tri = (object[])obj;
                    //TODO: кодов имени может быть много...
                    if ((int)tri[1] != name_code) return Enumerable.Empty<int>();
                    object[] pair = (object[])tri[2];
                    int tg = (int)pair[0];
                    string data = null;
                    if (tg == 2) data = (string)pair[1];
                    //else if (tg == 5) data = (string)((object[])pair[1])[1]; // пока будем работать только с простыми строками
                    if (data != null) return Enumerable.Repeat<int>(Hashfunctions.First4charsRu(data), 1);
                    return Enumerable.Empty<int>();
                },
                //new int[] { Hashfunctions.First4charsRu((string)((object[])obj)[1]) }
                comp);
        }

        public void Build(IEnumerable<object> triples)
        {
            Load(triples);
            s_index.Build();
            i_index.Build();
            name_index.Build();
            nt.Build();
            nt.Flush();
        }
        private void Load(IEnumerable<object> triples)
        {
            table.Clear();
            nt.Clear();
            foreach (object tri in triples)
            {
                long off = table.AppendElement(tri);
            }
            table.Flush();
            nt.Flush();
        }
        public void Refresh()
        {
            s_index.Refresh();
            table.Refresh();
            i_index.Refresh();
            name_index.Refresh();
            nt.Refresh();
        }
        public IEnumerable<object> GetBySubj(int subj)
        {
            return s_index.GetAllBySample(new object[] { subj, -1, null });
        }
        public IEnumerable<object> GetByObjName(string s)
        {
            return name_index.GetAllByKey(Hashfunctions.First4charsRu(s));
        }
        public IEnumerable<object> GetInverse(int obj)
        {
            return i_index.GetAllByKey(obj);
        }

        // ================== Утилиты ====================
        // Генерация потока триплетов
        internal IEnumerable<object> GenerateTripleFlow(IEnumerable<object> triples)
        {
            foreach (object[] tri in triples)
            {
                int subj = nt.GetSetStr((string)tri[0]);
                int pred = nt.GetSetStr((string)tri[1]);
                int tg = (int)((object[])tri[2])[0];
                if (tg == 1)
                {
                    int oobj = nt.GetSetStr((string)((object[])tri[2])[1]);
                    yield return new object[] { subj, pred, new object[] { 1, oobj } };
                }
                else
                {
                    string dobj = (string)((object[])tri[2])[1];
                    yield return new object[] { subj, pred, new object[] { 2, dobj } };
                }
            }
        }
        internal string DecodeTriple(object[] tr)
        {
            string subj = nt.Decode((int)tr[0]);
            string pred = nt.Decode((int)tr[1]);
            int tg = (int)((object[])tr[2])[0];
            object v = ((object[])tr[2])[1];
            return "<" + subj + "> <" + pred + "> " +
                (tg == 1 ? "<" + nt.Decode((int)v) + ">" : "\"" + (string)v + "\"") +
                " .";
        }

        internal string ToStr(object obj)
        {
            object[] tri = (object[])obj;
            object[] ooo = (object[])tri[2];
            int tg = (int)ooo[0];
            return "<" + nt.Decode((int)tri[0]) + "> <" + nt.Decode((int)tri[1]) + "> " +
            (tg == 1 ? "<" + nt.Decode((int)ooo[1]) + ">" : "\"" + ooo[1] + "\"") +
            ".";
        }
        public bool TryGetCode(string s, out int code)
        {
            return nt.TryGetCode(s, out code);
        }

    }

}