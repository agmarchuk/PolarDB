using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Polar.DB;

namespace Universal
{
    public class Program
    {
        public static void Main()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            System.Random rnd = new Random();
            string path = "";

            Console.WriteLine("Start Universal/Main");
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));
            int fno = 0;
            Func<Stream> gen_stream = () => File.Open(path + "f" + (fno++) +".bin", FileMode.OpenOrCreate);
            //Stream f1 = File.Open(path + "f1.bin", FileMode.OpenOrCreate);
            //Stream f2 = File.Open(path + "f2.bin", FileMode.OpenOrCreate);

            Sequence keyvalue_seq = new Sequence(tp_person, gen_stream, new IIndex[]
            {
                new IndexKey32Immutable(gen_stream, obj => (int)((object[])obj)[0])
            }.ToList());
            //Func<object, int> keyFunc = ob => (int)((object[])ob)[0];

            
            int nelements = 10_000_000;
            Console.WriteLine($"Sequence of {nelements} elements");
            bool toload = true;
            if (toload)
            {
                sw.Restart();
                var query = Enumerable.Range(0, nelements).Select(i => new object[] { nelements - i - 1, "" + (nelements - i - 1), 33.3 });
                keyvalue_seq.Fill(query);
                sw.Stop();
                Console.WriteLine($"Load ok. duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                sw.Restart();
                keyvalue_seq.Prepare();
                sw.Stop();
                Console.WriteLine($"Prepare ok. duration={sw.ElapsedMilliseconds}");

            }

            int key = nelements * 2 / 3;
            var val = keyvalue_seq.GetElementByKey(key);
            Console.WriteLine(val==null? "null" : tp_person.Interpret(val));

            return;

            sw.Restart();
            int nprobe = 10000;
            for (int i=0; i < nprobe; i++)
            {
                key = rnd.Next(nelements);
                var v = keyvalue_seq.GetElementByKey(key);
                if (v==null) Console.WriteLine($"null for {key}");
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetElementByKey. duration={sw.ElapsedMilliseconds}");
            //Console.ReadKey();
        }
    }
}
