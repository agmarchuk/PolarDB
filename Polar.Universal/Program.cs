using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Polar.DB;

namespace Polar.Universal
{
    class Program
    {
        public static void Main()
        {
            //Main1();
            Main2();
            //Main3();
        }
        public static void Main3()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Random rnd = new Random();
            Console.WriteLine("Start Main3 of Universal tests");

            // Директория для данных и генератор потоков
            string path = @"D:\Home\data\uni\";
            int nom = 0;
            Func<Stream> GenStream = () => File.Open(path + "d" + nom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            // Создаем тип элемента последовательности, саму последовательность
            PType tp = new PTypeRecord(
                new NamedType("Id", new PType(PTypeEnumeration.sstring)),
                new NamedType("Tp", new PType(PTypeEnumeration.sstring)),
                new NamedType("Props", new PTypeSequence(new PTypeRecord(
                    new NamedType("Prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("Value", new PType(PTypeEnumeration.sstring))))),
                new NamedType("sentence", new PType(PTypeEnumeration.sstring)));
            USequence sequence = new USequence(tp, GenStream,
                ob => (string)((object[])ob)[1] == "deleted",
                ob => (string)((object[])ob)[0],
                ke => Hashfunctions.HashRot13((string)ke));
            sequence.uindexes = new IUIndex[]
            {
            };

            // Загрузим последовательность тестовым материалом
            int nelements = 10;
            bool toload = true;
            if (toload)
            {
                sw.Start();
                var flow = Enumerable.Range(0, nelements).Select(ii =>
                {
                    nom = nelements - ii - 1;
                    object[] rec = new object[] 
                    { 
                        "" + nom, 
                        "person", 
                        new object[] { new object[] { "name", "n" + nom } }, 
                        "" 
                    };
                    return rec;
                });
                sequence.Load(flow);
                sequence.Flush();
                sw.Stop(); Console.WriteLine("load ok. duration=" + sw.ElapsedMilliseconds);
            }
            else
            {
                sequence.Refresh();
            }

            int key = nelements * 2 / 3;
            var va = sequence.GetByKey("" + key);
            Console.WriteLine(tp.Interpret(va));

            int nprobe = 1000;
            sw.Restart();
            for (int i=0; i<nprobe; i++)
            {
                int k = rnd.Next(nelements);
                var v = sequence.GetByKey("" + k);
            }
            sw.Stop(); Console.WriteLine($"{nprobe} gets ok. duration=" + sw.ElapsedMilliseconds);

            sequence.AppendElement(new object[] { "2", "deleted", new object[0], "" });
            sequence.AppendElement(new object[] { "4", "person", new object[0], "" });
            sequence.AppendElement(new object[] { "aaa", "person", new object[0], "" });

            foreach (var v in sequence.ElementValues())
            {
                Console.WriteLine(tp.Interpret(v));
            }
        }


        /// <summary>
        /// Тест на слабую динамику
        /// </summary>
        public static void Main2()
        {
            Random rnd = new Random();
            Console.WriteLine("Start Universal Dynamic!");
            string path = @"D:\Home\data\uni\";
            int nom = 0;
            Func<Stream> GenStream = () => File.Open(path + "d" + nom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            // ========= Создадим небольшую последовательность и динамически поработаем с ней.
            // В последовательности будет 2 поля целое и булевское, целое - ключ, булевское - признак deleted
            PType tp = new PTypeRecord(
                new NamedType("key", new PType(PTypeEnumeration.integer)),
                new NamedType("deleted", new PType(PTypeEnumeration.boolean)),
                new NamedType("value", new PType(PTypeEnumeration.sstring)),
                new NamedType("sentence", new PType(PTypeEnumeration.sstring))
                );
            USequence seq = new USequence(tp, GenStream, 
                ob => (bool)((object[])ob)[1],
                ob => (int)((object[])ob)[0], 
                ob => (int)ob);
            seq.uindexes = new IUIndex[] 
            { 
                new SVectorIndex(GenStream, seq,  
                    ob =>
                    {
                        string sentence = (string)((object[])ob)[3];
                        string[] words = sentence.Split(' ');
                        return words;
                    }),
                new SVectorIndex(GenStream, seq,
                    ob =>
                    {
                        string name = (string)((object[])ob)[2];
                        return new string[] { name };
                    })
            };
            seq.Load(new object[]
            {
                new object[] { 1, false, "first", "это был вопрос всех вопросов" },
                new object[] { 2, false, "second", "куда пойти учиться" },
                new object[] { 3, false, "third", "другой вопрос" },
                new object[] { 4, false, "fourth", "был про это" },
                new object[] { 5, false, "fifth", "и про то" },
                new object[] { 6, false, "sixth", "и про все все все" },
                new object[] { 77, false, "second", "как это все случилось" },
                new object[] { 8, false, "eight", "в какие вечера" },
                new object[] { 9, false, "nine", "три года ты мне снилась" },
                new object[] { 10, false, "ten", "а встретилась вчера" }
            });
            var val = seq.GetByKey(5);
            Console.WriteLine(tp.Interpret(val));

            seq.AppendElement(new object[] { 5, true, "", "" });
            seq.AppendElement(new object[] { 77, false, "seventy seven", "" });
            Console.WriteLine();
            foreach (var v in seq.ElementValues())
            {
                Console.WriteLine(tp.Interpret(v));
            }
            Console.WriteLine();

            Comparer<object> comp_like = Comparer<object>.Create(new Comparison<object>((object v1, object v2) =>
            {
                string a = (string)((object[])v1)[2];
                string b = (string)((object[])v2)[2];
                if (string.IsNullOrEmpty(b)) return 0;
                int len = b.Length;
                return string.Compare(
                    a, 0,
                    b, 0, len, StringComparison.OrdinalIgnoreCase);
            }));

            var qu = seq.GetAllByValue(0, "это");
            foreach (object[] r in qu)
            {
                Console.WriteLine($"{r[0]} {r[1]} {r[2]} {r[3]}");
            }
            Console.WriteLine("ok.");

            var q3 = seq.GetAllByLike(1, "s");
            foreach (object[] r in q3)
            {
                Console.WriteLine($"{r[0]} {r[1]} {r[2]} {r[3]}");
            }
            Console.WriteLine("ok.");

        }

        /// Тест работоспособности универсальной последовательности. И измерение скорости загрузки и выборки
        public static void Main1()
        {
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            Console.WriteLine("Start Universal!");
            string path = @"D:\Home\data\uni\";

            int nom = 0;
            Func<Stream> GenStream = () => File.Open(path +"f" + nom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            Comparer<object> name_direct = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[1];
                string val2 = (string)((object[])b)[1];
                return string.Compare(val1, val2, StringComparison.OrdinalIgnoreCase);
            }));

            USequence sequ = new USequence(
                new PTypeRecord(
                    new NamedType("id", new PType(PTypeEnumeration.sstring)),
                    new NamedType("name", new PType(PTypeEnumeration.sstring)),
                    new NamedType("age", new PType(PTypeEnumeration.integer))),
                GenStream, 
                rec => false,
                rec => (string)((object[])rec)[0],
                id => Hashfunctions.HashRot13((string)id));
            sequ.uindexes = new IUIndex[]
            {
                new UVectorIndex(GenStream, sequ, new PType(PTypeEnumeration.sstring),
                    ob =>
                    {
                        string sentence = (string)((object[])ob)[1];
                        string[] words = sentence.Split(' ');
                        return words;
                    }),
                new UVectorIndex(GenStream, sequ, new PType(PTypeEnumeration.sstring),
                    ob =>
                    {
                        string name = (string)((object[])ob)[1];
                        return new string[] { name };
                    })
            };

            int nrecords = 1_000_000;
            
            bool toload = true;
            if (toload)
            {
                sw.Restart();
                sequ.Clear();
                sequ.Load(
                    Enumerable.Range(0, nrecords)
                    .Select(ii => new object[] { "" +(nrecords - ii - 1), "n" + (nrecords - ii - 1), 26 })
                    );
                sw.Stop();
                Console.WriteLine($"Load ok. duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                sw.Restart();
                sequ.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh ok. duration={sw.ElapsedMilliseconds}");
            }


            int key = nrecords / 3 * 2;
            object[] v = (object[])sequ.GetByKey("" + key);
            Console.WriteLine($"{v[0]} {v[1]} {v[2]} ");

            sw.Restart();
            int nprobes = 1000;
            for (int i=0; i<nprobes; i++)
            {
                key = rnd.Next(nrecords);
                v = (object[])sequ.GetByKey("" + key);
                if (v == null) Console.WriteLine($"null using key={key}");
                if (v != null && (string)v[0] != "" + key)  Console.WriteLine($"{v[0]} {v[1]} {v[2]} ");
            }
            sw.Stop();
            Console.WriteLine($"{nprobes} gets ok. duration={sw.ElapsedMilliseconds}");

            sw.Restart();
            int cnt = 0;
            for (int i=0; i<nprobes; i++)
            {
                int k = rnd.Next(nrecords);
                string sample = "n" + k;
                var vals = sequ.GetAllByLike(0, sample).ToArray();
                cnt += vals.Count();
                if (vals.Count() > 4) foreach (object[] vv in vals) Console.WriteLine($"{vv[0]} {vv[1]} {vv[2]} ");
            }
            sw.Stop(); Console.WriteLine($"duration: {sw.ElapsedMilliseconds} cnt={cnt}");
        }
    }
}
