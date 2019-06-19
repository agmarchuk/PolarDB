using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;

using Polar.TripleStore;

namespace GetStarted
{
    partial class Program
    {
        public static void Main20()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            System.Random rnd = new Random();
            //string path = "../../../../data/Databases/";
            string path = "D:/Home/data/Databases/";
            int fnom = 0;
            Func<Stream> GenStream = () => File.Open(path + (fnom++), FileMode.OpenOrCreate);
            Console.WriteLine("Start TestConsoleApp (Main32)");
            TripleRecordStore store = new TripleRecordStore(GenStream, path, new string[] {
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                , "http://fogid.net/o/name"
                , "http://fogid.net/o/age"
                , "http://fogid.net/o/person"
                , "http://fogid.net/o/photo-doc"
                , "http://fogid.net/o/reflection"
                , "http://fogid.net/o/reflected"
                , "http://fogid.net/o/in-doc"
            });

            int npersons = 4_000_000;
            int nphotos = npersons * 2;
            int nreflections = npersons * 6;

            bool toload = false;
            bool tocode = true;
            if (toload)
            {
                sw.Restart();
                store.Clear();
                var persons = Enumerable.Range(0, npersons).Select(i => npersons - i - 1)
                    .Select(c => new object[] { -c-1, // Это "прямой" код не требующий кодирования через таблицу
                        new object[] { new object[] { 0, 3 } },
                        new object[] { new object[] { 1, "p" + c }, new object[] { 2, "" + 33 } }
                    });
                var persons2 = Enumerable.Range(0, npersons).Select(i => npersons - i - 1)
                    .Select(c => store.CodeRecord(new object[] { ""+c,
                        new object[] { new object[] { "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://fogid.net/o/person" } },
                        new object[] { new object[] { "http://fogid.net/o/name", "p" + c }, new object[] { "http://fogid.net/o/age", "" + 33 } }
                    }));
                var persons3 = Enumerable.Range(0, npersons).Select(i => npersons - i - 1)
                    .Select(c => store.CodeRecord(new object[] { ""+c, // Это "прямой" код не требующий кодирования через таблицу
                        new object[] { new object[] { 0, 3 } },
                        new object[] { new object[] { 1, "p" + c }, new object[] { 2, "" + 33 } }
                    }));
                var photos = Enumerable.Range(0, nphotos).Select(i => nphotos - i - 1)
                    .Select(c => new object[] { -(c+npersons)-1, // Это "прямой" код не требующий кодирования через таблицу
                        new object[] { new object[] { 0, 4 } },
                        new object[] { new object[] { 1, "f" + c } }
                    });
                var photos2 = Enumerable.Range(0, nphotos).Select(i => nphotos - i - 1)
                    .Select(c => store.CodeRecord(new object[] { ""+(c+npersons),
                        new object[] { new object[] { "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://fogid.net/o/photo-doc" } },
                        new object[] { new object[] { "http://fogid.net/o/name", "f" + c } }
                    }));
                var photos3 = Enumerable.Range(0, nphotos).Select(i => nphotos - i - 1)
                    .Select(c => store.CodeRecord(new object[] { ""+(c+npersons), // Это "прямой" код не требующий кодирования через таблицу
                        new object[] { new object[] { 0, 4 } },
                        new object[] { new object[] { 1, "f" + c } }
                    }));
                var reflections = Enumerable.Range(0, nreflections).Select(i => nreflections - i - 1)
                    .Select(c => new object[] { -(c+3*npersons)-1, // Это "прямой" код не требующий кодирования через таблицу
                        new object[] { new object[] { 0, 5 },
                            new object[] { 6, -1 - (rnd.Next(npersons)) },
                            new object[] { 7, -1 - (rnd.Next(nphotos) + npersons) } },
                        new object[] { }
                    });
                var reflections2 = Enumerable.Range(0, nreflections).Select(i => nreflections - i - 1)
                    .Select(c => store.CodeRecord(new object[] { "" +(c+3*npersons),
                        new object[] { new object[] { "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://fogid.net/o/reflection" },
                            new object[] { "http://fogid.net/o/reflected", ""+rnd.Next(npersons) },
                            new object[] { "http://fogid.net/o/in-doc", ""+(rnd.Next(nphotos) + npersons) } },
                        new object[] { }
                    }));
                var reflections3 = Enumerable.Range(0, nreflections).Select(i => nreflections - i - 1)
                    .Select(c => store.CodeRecord(new object[] { "" +(c+3*npersons), // Это "прямой" код не требующий кодирования через таблицу
                        new object[] { new object[] { 0, 5 },
                            new object[] { 6, ""+rnd.Next(npersons) },
                            new object[] { 7, ""+(rnd.Next(nphotos) + npersons) }
                        },
                        new object[] { }
                    }));
                if (tocode)
                {
                    store.Load(persons2.Concat(photos2).Concat(reflections2));
                    //store.Load(persons3.Concat(photos3).Concat(reflections3));
                }
                else
                {
                    store.Load(persons.Concat(photos).Concat(reflections));
                }
                store.Build();
                sw.Stop();
                Console.WriteLine($"Load ok. duration={sw.ElapsedMilliseconds}");
            }
            //else
            {
                sw.Restart();
                store.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh for Phototeka {npersons} persons. Duration={sw.ElapsedMilliseconds}");
            }


            // ПОлукодирование иногда кодирование, иногда нет
            Func<int, int> HCode = nom => tocode ? store.Code("" + nom) : -1 - nom;
            // Проверка
            int code = HCode(npersons * 2 / 3);

            var query1 = store.GetRecord(code);
            Console.WriteLine(store.ToTT(query1));

            // Скорость выполнения запросов
            int nprobe = 10000;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int c = HCode(rnd.Next(npersons));
                var ob = store.GetRecord(c);
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetRecord() ok. duration={sw.ElapsedMilliseconds}");

            // Надо найти все фотографии, в которых отражается персона с выбранным (случайно) кодом. 
            sw.Restart();
            int total = 0;
            nprobe = 1000;
            for (int i = 0; i < nprobe; i++)
            {
                int c = HCode(rnd.Next(npersons));
                var query2 = store.GetRefers(c).Cast<object[]>().ToArray();
                var q3 = query2
                    .Select(ob => ((object[])ob[1]).Cast<object[]>()
                        .First(dupl => (int)dupl[0] == 7));
                var q4 = q3
                    .Select(bb => store.GetRecord((int)bb[1])).ToArray();
                total += query2.Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} persons for photos ok. duration={sw.ElapsedMilliseconds} total={total}");

            //var likes = store.Like("p" + 666);
            //foreach (var like in likes)
            //{
            //    Console.WriteLine($"like {store.ToTT(like)}");
            //}

            sw.Restart();
            total = 0;
            for (int i=0; i<1000; i++)
            {
                string id = "p" + rnd.Next(npersons / 10 + 1, npersons);
                total += store.Like(id).Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} likes for persons ok. duration={sw.ElapsedMilliseconds} total={total}");
        }
    }

}
