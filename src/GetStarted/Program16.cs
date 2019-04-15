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

            // Проверка таблицы имен
            Nametable32 nt = new Nametable32(GenStream);

            int npersons = 4_000_000;
            bool toload = true;

            if (toload)
            {
                nt.Clear();
                nt.GetSetStr("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

                IEnumerable<object> qu_persons = Enumerable.Range(0, npersons)
                    .SelectMany(i =>
                    {
                        return new object[] {
                                        new object[] { "p" + i, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                            new object[] { 1, "http://fogid.net/o/person"}},
                                        new object[] { "p" + i, "http://fogid.net/o/name",
                                            new object[] { 1, "p"+i}},
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
                foreach (object[] tri in qu_persons.Concat(qu_fotos).Concat(qu_reflections))
                {
                    int subj = nt.GetSetStr((string)tri[0]);
                    int pred = nt.GetSetStr((string)tri[1]);
                    int tg = (int)((object[])tri[2])[0];
                    if (tg == 1)
                    {
                        int obj = nt.GetSetStr((string)((object[])tri[2])[1]);
                    }
                    else
                    {

                    }
                    //Console.WriteLine(code);
                }
                nt.Flush();
                sw.Stop();
                Console.WriteLine($"Duration={sw.ElapsedMilliseconds}");

                sw.Restart();
                nt.Build();
                sw.Stop();
                Console.WriteLine($"Duration={sw.ElapsedMilliseconds}");
            }



            for (int i = 77; i < 87; i++)
            {
                string id = "p" + i;
                if (!nt.TryGetCode(id, out int cod)) throw new Exception("292");
                Console.WriteLine($"id={id} cod={cod}");
            }
            return;
            // конец проверки

            TripleStoreInt32 store = new TripleStoreInt32(GenStream);
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

            int nprobe = 1000;

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
            int total = 0;
            for (int i = 0; i < nprobe; i++)
            {
                int obj = rnd.Next(nelements);
                var quer = store.GetInverse(obj);
                total += quer.Count();
            }
            sw.Stop();
            Console.WriteLine($"inverse elements {nprobe} GetInverse search ok. duration={sw.ElapsedMilliseconds} total = {total}");

            return;

            var qu2 = store.GetByObjString("p12345");
            foreach (object[] t in qu2)
            {
                Console.WriteLine($"{t[0]} {t[1]}");
            }


            nprobe = 1000;
            sw.Restart();
            total = 0;
            for (int i = 0; i < nprobe; i++)
            {
                var quer = store.GetByObjString("" + rnd.Next(nelements));
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
        // Носителем таблицы является последовательность строк. Номер строки - ее код. Это первично. 
        // По коду строка определяется однозначно (как вводили), по строке код может определяться с учетом эквивалентностей.
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
        public void Load(IEnumerable<string> flow)
        {
            // а правильно ли без очистки name_index ?
            int cod = (int)table.Count();
            foreach (string s in flow)
            {
                long off = table.AppendElement(new object[] { cod, s });
                cod++;
                str_offsets.AppendElement(off);
            }
            table.Flush();
            str_offsets.Flush();
        }
        public void Build()
        {
            // Про порядок операторов еще надо подумать
            dyna_index = new Dictionary<string, int>();
            name_index.Build();
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
            var q = name_index.GetAllBySample(new object[] { -1, s }).FirstOrDefault();
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
    }

    public class TripleStoreInt32
    {
        private UniversalSequenceBase table;
        private IndexKey32CompImm s_index;
        private IndexKey32CompImm o_index;
        private IndexKey32Imm i_index;
        public static Func<object, int> Test_keyfun = null;
        public TripleStoreInt32(Func<Stream> stream_gen)
        {
            // Тип Object Variants
            PType tp_ov = new PTypeUnion(
                new NamedType("dummy", new PType(PTypeEnumeration.none)),
                new NamedType("iri", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)),
                new NamedType("int", new PType(PTypeEnumeration.sstring)),
                new NamedType("date", new PType(PTypeEnumeration.sstring)),
                new NamedType("langstr", new PType(PTypeEnumeration.sstring))
                );
            PType tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            table = new UniversalSequenceBase(tp_triple, stream_gen());
            s_index = new IndexKey32CompImm(stream_gen, table, ob => (int)((object[])ob)[0], null);
            // Специальное кодирование. В принципе, все расположено почти по естественному порядку. Исключение - группа [\\]^_`
            string selected_chars = "!\"#$%&\'()*+,-./0123456789:;<=>?@[\\]^_`abcdefghjklmnopqrstuwxyz{|}~абвгдежзийклмнопрстуфхцчшщъыьэюяё";
            // Полуключевая функция. Преобразует объект триплета в 32-разрядное число. код iri сохраняется, предполагается
            // что старший разряд 0. Если не ноль, тогда другие варианты, пока сделаю только строку 
            Func<object, int> halfKeyFun = ob =>
            {
                object[] tri = (object[])ob;
                object[] pair = (object[])tri[2];
                int tg = (int)pair[0];
                if (tg == 1) // iri
                {
                    return ((int)pair[1]) & ~(1<<31); 
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
                    // вместо 0 будет вариант строки
                    return (1<<31) | (0) | ((((((chs[0] << 7) | chs[1]) << 7) | chs[2]) << 7) | chs[3]);
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
            i_index = new IndexKey32Imm(stream_gen, table, obj =>
            {
                object[] pair = (object[])((object[])obj)[2];
                int tg = (int)pair[0];
                if (tg != 1) return Enumerable.Empty<int>();
                return Enumerable.Repeat<int>((int)pair[1], 1);
            }, null);
        }
        public void Build(IEnumerable<object> triples)
        {
            Load(triples);
            s_index.Build();
            i_index.Build();
            //o_index.Build();
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
            i_index.Refresh();
        }
        public IEnumerable<object> GetBySubj(int subj)
        {
            return s_index.GetAllBySample(new object[] { subj, -1, null });
        }
        public IEnumerable<object> GetByObjString(string s)
        {
            return o_index.GetAllBySample(new object[] { -1, -1, new object[] { 2, s } });
        }
        public IEnumerable<object> GetInverse(int obj)
        {
            return i_index.GetAllByKey(obj);
        }

    }

}