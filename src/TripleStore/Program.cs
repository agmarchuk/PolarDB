using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Polar.DB;

namespace TripleStore
{
    class Program
    {
        static void Main(string[] args)
        {
            string path = "/Home/data";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Random rnd = new Random();
            //Random rnd = new Random(11111);
            Console.WriteLine("Start TripleStore experiments");
            int cnt = 0;
            Func<Stream> GenStream = () => new FileStream(path + "/f" + (cnt++) + ".bin",
                FileMode.OpenOrCreate, FileAccess.ReadWrite);

            TripleStoreInt32 store = new TripleStoreInt32(GenStream, path);

            int npersons = 40_000;
            bool toload = true;

            if (toload)
            {
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

                var triples_set = qu_persons.Concat(qu_fotos).Concat(qu_reflections);

                if (true)
                {
                    Dictionary<string, int> dic = new Dictionary<string, int>();
                    UniversalSequenceBase usb = new UniversalSequenceBase(
                        new PTypeRecord(
                            new NamedType("code", new PType(PTypeEnumeration.integer)),
                            new NamedType("str", new PType(PTypeEnumeration.sstring))),
                        GenStream());
                    UniversalSequenceBase offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger),
                        GenStream());

                    List<object> tlist = new List<object>();
                    usb.Clear();
                    offsets.Clear();

                    Func<string, int> todic, todic2;
                    todic = s =>
                     {
                         int nom = -1;
                         if (!dic.TryGetValue(s, out nom))
                         {
                             nom = (int)usb.Count();
                             long off = usb.AppendElement(new object[] { nom, s });
                             offsets.AppendElement(off);
                             dic.Add(s, nom);
                         }
                         return nom;
                     };

                    Nametable32 nt = new Nametable32(GenStream);
                    nt.Clear();
                    todic = s =>
                    {
                        return nt.GetSetStr(s);
                    };

                    sw.Restart();
                    foreach (object[] tri in triples_set)
                    {
                        string s = (string)tri[0];
                        int subj = todic(s);
                        s = (string)tri[1];
                        int pred = todic(s);
                        object[] obj = (object[])tri[2];
                        object[] o;
                        if ((int)obj[0] == 1)
                        {
                            o = new object[] { (int)obj[0], todic((string)obj[1]) };
                        }
                        else
                        {
                            o = new object[] { (int)obj[0], (string)obj[1] };
                        }
                        //tlist.Add(new object[] { subj, pred, o });
                    }
                    usb.Flush();
                    offsets.Flush();
                    sw.Stop();
                    Console.WriteLine($"store Build ok. Duration={sw.ElapsedMilliseconds}");

                    return;
                }

                sw.Restart();
                store.Build(triples_set);
                sw.Stop();
                Console.WriteLine($"store Build ok. Duration={sw.ElapsedMilliseconds}");

                return;
            }
            else
            {
                sw.Restart();
                store.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh for Phototeka {npersons} persons. Duration={sw.ElapsedMilliseconds}");
            }


            // Испытываю
            Console.WriteLine("===== single GetBySubject =====");
            // Беру персону 
            string id = "p" + (npersons * 2 / 3);
            // Проверяю работу выборки по субъекту
            var trs = store.Get_s(id);
            // Печатаю
            foreach (object[] t in trs) Console.WriteLine(store.TripleToString(t));

            Console.WriteLine("===== single GetInverse =====");

            var trs2 = store.Get_t(id);
            // Печатаю
            foreach (object[] t in trs2) Console.WriteLine(store.TripleToString(t));

            Console.WriteLine("===== 1000 GetBySubject =====");

            sw.Restart();
            int ntriples = 0;
            for (int i=0; i<1000; i++)
            {
                string c = "p" + rnd.Next(npersons);
                ntriples += store.Get_s(c).Count();
            }
            sw.Stop();
            Console.WriteLine($"ok. duration={sw.ElapsedMilliseconds}");

            int pers = (npersons * 2 / 30);
            //pers = 243; //24;
            Console.WriteLine($"===== single Like {"p" + pers} =====");
            var trs3 = store.Like("p" + pers).ToArray();
            // Печатаю
            foreach (object[] t in trs3) Console.WriteLine(store.TripleToString(t));

            if (!store.TryGetCode(id, out int nid)) throw new Exception("222233");
                //var trs2 = store.GetInverse(nid);
                //// Печатаю
                //foreach (object[] t in trs2) Console.WriteLine(store.DecodeTriple(t));

            //    return;

            int reflected, indoc, name;

            Console.WriteLine("===== Complex query =====");

            if (!store.TryGetCode("http://fogid.net/o/reflected", out reflected)) throw new Exception("338434");
            if (!store.TryGetCode("http://fogid.net/o/indoc", out indoc)) throw new Exception("338435");
            if (!store.TryGetCode("http://fogid.net/o/name", out name)) throw new Exception("338436");
            var query1 = store.Get_t(nid)
                .Cast<object[]>()
                .Where(t => (int)t[1] == reflected)
                .SelectMany(t => store.Get_s((int)t[0])
                    .Where(tt => (int)((object[])tt)[1] == indoc)
                    .Select(tt => store.Get_s((int)((object[])((object[])tt)[2])[1])
                        .Where(ttt => (int)((object[])ttt)[1] == name)
                        .FirstOrDefault())
                    )

                ;
            foreach (object[] t in query1)
            {
                Console.WriteLine(store.TripleToString(t));
            }
            Console.WriteLine();

            Console.WriteLine("===== 1000 GetBySubject =====");

            int nprobe = 1000;
            int total = 0;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                // Беру случайную персону
                if (!store.TryGetCode("p" + rnd.Next(npersons), out nid)) throw new Exception("338437");
                var fots = store.Get_t(nid)
                    //.Cast<object[]>()
                    //.Where(t => (int)t[1] == reflected)
                    //.SelectMany(t => store.Get_s((int)t[0])
                    //    .Where(tt => (int)((object[])tt)[1] == indoc)
                    //    .Select(tt => store.Get_s((int)((object[])((object[])tt)[2])[1])
                    //        .Where(ttt => (int)((object[])ttt)[1] == name)
                    //        .FirstOrDefault()))
                            ;
                total += fots.Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} tests ok. Duration={sw.ElapsedMilliseconds} total={total}");
 
/*
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
*/
        }
    }
}
