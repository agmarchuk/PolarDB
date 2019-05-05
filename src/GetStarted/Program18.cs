using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using Polar.DB;

namespace GetStarted
{
    public partial class Program
    {
        public static void Main18()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Random rnd = new Random();
            string path = "";
            Console.WriteLine("Start Main18: bearing experiments");
            int cnt = 0;
            Func<Stream> GenStream = () => new FileStream(path + "Databases/f" + (cnt++) + ".bin",
                FileMode.OpenOrCreate, FileAccess.ReadWrite);
            PType tp_elem = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));

            BearingPure table = new BearingPure(tp_elem, GenStream);
            table.Indexes = new IIndex[]
                {
                    new IndexKey32CompImmutable(GenStream, table, obj => 
                        Enumerable.Repeat((int)((object[])obj)[0], 1), null)
                };

            int nelements = 1_000_000;
            bool toload = true;

            if (toload)
            {
                sw.Restart();
                table.Clear();
                int n = nelements;
                table.Load(
                    Enumerable.Range(0, nelements)
                        .Select(i => new object[] { (n - i - 1), ""+(n - i - 1), 33 })
                );
                sw.Stop();
                Console.WriteLine($"load {nelements} ok. duration={sw.ElapsedMilliseconds}");
            }

            int key = nelements * 2 / 3;
            IndexKey32CompImmutable id_index = (IndexKey32CompImmutable)table.Indexes[0];
            var obs = id_index.GetAllByKey(key);
            foreach (var ob in obs)
            {
                Console.WriteLine(tp_elem.Interpret(ob));
            }

            int nprobes = 1000;
            sw.Restart();
            int total = 0;
            for (int i=0; i<nprobes; i++)
            {
                int ke = rnd.Next(nelements);
                total += id_index.GetAllByKey(ke).Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobes} probes ok. duration={sw.ElapsedMilliseconds} total={total}");
        }
    }
}
