using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Polar.DB;

namespace Polar.OModel
{
    class Program
    {
        static void Main_()
        {
            Main4();
        }


        static void Main4()
        {
            Console.WriteLine("Start RRecordsUniversal (4)");

            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            PType tp_prop = new PTypeUnion(
                new NamedType("novariant", new PType(PTypeEnumeration.none)),
                new NamedType("field", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("value", new PType(PTypeEnumeration.sstring)),
                    new NamedType("lang", new PType(PTypeEnumeration.sstring)))),
                new NamedType("objprop", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("link", new PType(PTypeEnumeration.sstring))))
                );
            PType tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.sstring)),
                new NamedType("tp", new PType(PTypeEnumeration.sstring)),
                new NamedType("props", new PTypeSequence(tp_prop)));

            object[] val = new object[]
            {
                "Pupkin", "person",
                new object[]
                {
                    new object[] { 1, new object[] { "name", "Пупкин", "ru"}},
                    new object[] { 2, new object[] { "father", "pupkin_father"}}
                }
            };

            Console.WriteLine($"{tp_rec.Interpret(val)}");

            // Генератор стримов
            string path = @"D:\Home\data\US\";
            int snom = 0;
            Func<Stream> streamGen = () => new FileStream(path + "fs" + snom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            // Определяем хеш-функции и компараторы
            // Сначала - для поиска по id
            Func<object, int> hashId = obj => Hashfunctions.HashRot13(((string)(((object[])obj)[0])));
            Comparer<object> comp_direct = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[0];
                string val2 = (string)((object[])b)[0];
                return string.Compare(val1, val2, StringComparison.OrdinalIgnoreCase);
            }));

            // Создаем последовательность R-записей
            UniversalSequence sequence = new UniversalSequence(tp_rec, streamGen, null, null,
                obj => Hashfunctions.HashRot13((string)(((object[])obj)[0])),
                comp_direct,
                new HashComp[]
                { 
                    //new HashComp { Hash = null, Comp = comp_direct } 
                });
            SVectorIndex svi = new SVectorIndex(streamGen, sequence, obj =>
            {
                object[] props = (object[])((object[])obj)[2];
                var query = props.Where(p => (int)((object[])p)[0] == 1)
                    .Select(p => ((object[])p)[1])
                    .Cast<object[]>()
                    .Where(f => (string)f[0] == "name")
                    .Select(f => (string)f[1]).ToArray();
                return query;
            });

            int npersons = 100_000;
            bool toload = true;
            if (toload)
            {
                sequence.Load(
                    Enumerable.Range(0, npersons).Select(i =>
                        new object[]
                        {"p" + i, "person", new object[]
                            {
                                new object[] { 1, new object[] { "name", "n" + i, "ru"}}
                            }
                        })
                    );
            }

            int k = npersons / 3 * 2;
            var q = (object[])sequence.GetByKey(new object[] { "p" + k, null, null });
            Console.WriteLine($"{tp_rec.Interpret(q)}");
            Console.WriteLine();

            k = npersons / 3 * 2 / 10;
            var several = svi.LikeBySKey("n" + k).ToArray();
            foreach (var v in several)
            {
                Console.WriteLine($"{tp_rec.Interpret(v)}");
            }

            int nprobes = 1000;
            int sum = 0;
            sw.Restart();
            for (int i = 0; i < nprobes; i++)
            {
                int key = rnd.Next(npersons);
                var qu = svi.LikeBySKey("n" + key);
                sum += qu.Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobes} likes ok. sum={sum} duration={sw.ElapsedMilliseconds}");

            // 124 мс на 1000 поисков
        }

        static void Main3()
        {
            Console.WriteLine("Start Universal");

            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            PType tp = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.sstring)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            Console.WriteLine($"{tp.Interpret(new object[] { "777", "Иванов", 33 })}");

            // Генератор стримов
            string path = @"D:\Home\data\US\";
            int snom = 0;
            Func<Stream> streamGen = () => new FileStream(path + "fs" + snom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            Comparer<object> comp_direct = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[0];
                string val2 = (string)((object[])b)[0];
                return string.Compare(val1, val2, StringComparison.OrdinalIgnoreCase);
            }));


            // Создаем последовательность
            UniversalSequence usequence = new UniversalSequence(tp, streamGen, null, null,
                obj => Hashfunctions.HashRot13((string)((string)(((object[])obj)[0]))),
                comp_direct, new HashComp[] {  }
                );
            SVectorIndex svi = new SVectorIndex(streamGen, usequence, obj => new string[] { (string)((object[])obj)[1] });

            //obj => Polar.DB.Hashfunctions.HashRot13((string)(((object[])obj)[0])),

            // Заполняем данными
            int nelements = 10_000_000;
            Console.WriteLine("nelements=" + nelements);
            bool toload = false;
            if (toload)
            {
                sw.Restart();
                usequence.Clear();
                usequence.Load(
                    Enumerable.Range(0, nelements)
                    .Select(ip => new object[] { "" + (nelements - ip - 1), "p" + (nelements - ip - 1), 33 })
                    );
                sw.Stop();
                Console.WriteLine($"Load ok. duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                sw.Restart();
                usequence.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh ok. duration={sw.ElapsedMilliseconds}");
            }

            int k = 4;//nelements / 3 * 2;
            object[] q = (object[])usequence.GetByKey(new object[] { "" + k, null, -1 });
            Console.WriteLine($"{q[0]} {q[1]} {q[2]}");

            var query = svi.LikeBySKey("p" + (nelements / 3 * 2));
            Console.WriteLine("<<<");
            foreach (object v in query)
            {
                Console.WriteLine(tp.Interpret(v));
            }
            Console.WriteLine(">>>");

            rnd = new Random(777888);

            int nprobe = 1000;
            object[] el = null;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int key = rnd.Next(nelements);
                var qu = usequence.GetByKey(new object[] { "" + key, null, -1 });
                el = (object[])qu;
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} gets ok. duration={sw.ElapsedMilliseconds}");


            Comparer<string> comp_slike = Comparer<string>.Create(new Comparison<string>((string a, string b) =>
            {
                if (string.IsNullOrEmpty(b)) return 0;
                int len = b.Length;
                return string.Compare(
                    a, 0,
                    b, 0, len, StringComparison.OrdinalIgnoreCase);
            }));

            nprobe = 1000;

            int sum = 0;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int key = rnd.Next(nelements);
                //var qu = svi.GetBySKey("p2", comp_slike);
                var qu = svi.LikeBySKey("p" + key);
                //var qu = svi.GetBySKey("p2", null);
                sum += qu.Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} likes ok. sum={sum} duration={sw.ElapsedMilliseconds}");

            // Массив объектов 
            // 1 млн. Load 2 сек., gets 3 мсек
            // 10 млн. Load 20 сек., gets 4 мсек
            // 20 млн. Load 44 сек., gets 4 мсек. Захват ОЗУ 3.6 Гб

            // Массив хеш-ключей и массив офсетов
            // 1 млн. Load 0.7 сек., gets 9 мсек
            // 10 млн. Load 6 сек., gets 9 мсек
            // 20 млн. Load 12 сек., gets 9 мсек. Захват ОЗУ 3.6 Гб
            // 100 млн. Load 64 сек., gets 3.7 сек. Захват ОЗУ 2.5 Гб

            // Массив хеш-ключей и последовательность офсетов
            // 1 млн. Load 0.8 сек., gets 16 мсек
            // 10 млн. Load 6.5 сек., gets 16 мсек
            // 20 млн. Load 13 сек., gets 16 мсек. Захват ОЗУ 3.6 Гб
            // 100 млн. Load 75 сек., gets 15 сек. Захват ОЗУ 2.5 Гб

            // Массив хеш-ключей и последовательность офсетов, новая процедура поиска
            // 1 млн. Load 0.8 сек., gets 15 мсек
            // 10 млн. Load 7.9 сек., gets 18 мсек

            // Последовательность хеш-ключей и последовательность офсетов
            // 1 млн. Load 0.8 сек., gets 85 мсек
            // 10 млн. Load 7.8 сек., gets 117 мсек
            // 20 млн. Load 14 сек., gets 111 мсек. Захват ОЗУ 0.5 Гб
            // 100 млн. Load 102 сек., gets 19 сек. Захват ОЗУ 2.5 Гб, всего данных 3.23 Гб

            // Последовательность хеш-ключей и последовательность офсетов, новый индекс и процедура поиска Like
            // 1 млн. Load 2.5 сек., gets 78 мсек, Like 250
            // 10 млн. Load 26 сек., gets 108 мсек, Like 232 мсек
        }

        static void Main2()
        {
            Console.WriteLine("Start RRecordsUniversal");

            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            PType tp_prop = new PTypeUnion(
                new NamedType("novariant", new PType(PTypeEnumeration.none)),
                new NamedType("field", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("value", new PType(PTypeEnumeration.sstring)),
                    new NamedType("lang", new PType(PTypeEnumeration.sstring)))),
                new NamedType("objprop", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("link", new PType(PTypeEnumeration.sstring))))
                );
            PType tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.sstring)),
                new NamedType("tp", new PType(PTypeEnumeration.sstring)),
                new NamedType("props", new PTypeSequence(tp_prop)));

            object[] val = new object[]
            {
                "Pupkin", "person",
                new object[]
                {
                    new object[] { 1, new object[] { "name", "Пупкин", "ru"}},
                    new object[] { 2, new object[] { "father", "pupkin_father"}}
                }
            };

            Console.WriteLine($"{tp_rec.Interpret(val)}");

            // Генератор стримов
            string path = @"D:\Home\data\US\";
            int snom = 0;
            Func<Stream> streamGen = () => new FileStream(path + "fs" + snom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            // Определяем хеш-функции и компараторы
            // Сначала - для поиска по id
            Func<object, int> hashId = obj => Hashfunctions.HashRot13(((string)(((object[])obj)[0])));
            Comparer<object> comp_direct = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[0];
                string val2 = (string)((object[])b)[0];
                return string.Compare(val1, val2, StringComparison.OrdinalIgnoreCase);
            }));

            Comparer<object> comp_like = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[1];
                string val2 = (string)((object[])b)[1];
                if (string.IsNullOrEmpty(val2)) return 0;
                int len = val2.Length;
                return string.Compare(
                    val1, 0,
                    val2, 0, len, StringComparison.OrdinalIgnoreCase);
            }));

            // Создаем последовательность
            UniversalSequence sequence = new UniversalSequence(tp_rec, streamGen, null, null,
                obj => Hashfunctions.HashRot13((string)(((object[])obj)[0])),
                comp_direct, 
                new HashComp[] 
                { 
                    //new HashComp { Hash = null, Comp = comp_direct } 
                });
        }






        static void Main1()
        {
            Console.WriteLine("Start Universal");

            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            PType tp = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            Console.WriteLine($"{tp.Interpret(new object[] { 777, "Иванов", 33 })}");

            // Генератор стримов
            string path = @"D:\Home\data\US\";
            int snom = 0;
            Func<Stream> streamGen = () => new FileStream(path + "fs" + snom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            // Определяем компараторы
            Comparer<object> comp_like = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[1];
                string val2 = (string)((object[])b)[1];
                if (string.IsNullOrEmpty(val2)) return 0;
                int len = val2.Length;
                return string.Compare(
                    val1, 0,
                    val2, 0, len, StringComparison.OrdinalIgnoreCase);
            }));
            Comparer<object> comp_direct = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[1];
                string val2 = (string)((object[])b)[1];
                return string.Compare(val1, val2, StringComparison.OrdinalIgnoreCase);
            }));


            // Создаем последовательность
            UniversalSequence usequence = new UniversalSequence(tp, streamGen, null, null,
                obj => (int)(((object[])obj)[0]),
                null, new HashComp[] { new HashComp { Hash = null, Comp = comp_direct } }
                );

            //obj => Polar.DB.Hashfunctions.HashRot13((string)(((object[])obj)[0])),

            // Заполняем данными
            int nelements = 1_000_000;
            Console.WriteLine("nelements=" + nelements);
            bool toload = false;
            if (toload)
            {
                sw.Restart();
                usequence.Clear();
                usequence.Load(
                    Enumerable.Range(0, nelements)
                    .Select(ip => new object[] { nelements - ip - 1, "p" + (nelements - ip - 1), 33 })
                    );
                sw.Stop();
                Console.WriteLine($"Load ok. duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                sw.Restart();
                usequence.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh ok. duration={sw.ElapsedMilliseconds}");
            }

            int k = nelements / 3 * 2;
            object[] q = (object[])usequence.GetByKey(new object[] { k, null, -1 });
            Console.WriteLine($"{q[0]} {q[1]} {q[2]}");


            int nprobe = 10000;
            object[] el = null;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int key = rnd.Next(nelements);
                var qu = usequence.GetByKey(new object[] { key, null, -1 });
                el = (object[])qu;
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} gets ok. duration={sw.ElapsedMilliseconds}");

            // 1 млн. Load 0.8 сек., gets 22 мсек
            // 10 млн. Load 8.5 сек., gets 24 мсек
            // 20 млн. Load 16 сек., gets 24 мсек
            // 40 млн. Load 52 сек., gets 7 сек
            // 40 млн. без загрузки, gets 4 сек

            object[] sample = new object[] { -1, "p444", -1 };
            var query = usequence.LikeUsingIndex(0, sample, comp_like);
            foreach (object[] v in query)
            {
                //Console.WriteLine($"={v[0]} {v[1]} {v[2]}");
            }
        }
    }
}
