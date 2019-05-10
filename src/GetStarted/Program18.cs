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

            //BearingPure table = new BearingPure(tp_elem, GenStream);
            BearingDeletable table = new BearingDeletable(tp_elem, GenStream);
            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                return string.Compare((string)((object[])a)[1], (string)((object[])b)[1]);
            }));
            table.Indexes = new IIndex[]
                {
                    new IndexKey32Comp(GenStream, table, obj => true,
                        obj => (int)((object[])obj)[0], null),
                    new IndexView(GenStream, table, obj => true,
                        comp, path + "Databases/", 20_000_000)
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
            else
            {
                sw.Restart();
                table.Refresh();
                sw.Stop();
                Console.WriteLine($"refresh {nelements} ok. duration={sw.ElapsedMilliseconds}");
            }

            int key = nelements * 2 / 3;
            IndexKey32Comp id_index = (IndexKey32Comp)table.Indexes[0];
            var obs = id_index.GetAllByKey(key);
            Console.WriteLine("Test of IndexKey32Comp");
            foreach (var ob in obs)
            {
                Console.WriteLine(tp_elem.Interpret(ob));
            }


            IndexView name_index = (IndexView)table.Indexes[1];
            // Экспермент по поиску похожих
            Comparer<object> comp_like = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                int len = ((string)((object[])b)[1]).Length;
                return string.Compare((string)((object[])a)[1], 0, (string)((object[])b)[1], 0, len);
            }));

            Console.WriteLine("Test of SearchAll");
            var quer = name_index.SearchAll(new object[] { -1, ""+(key/10), -1 }, comp_like);
            foreach (var ob in quer)
            {
                Console.WriteLine("~~" + tp_elem.Interpret(ob));
            }

            int nprobes = 1000;

            sw.Restart();
            int total = 0;
            for (int i = 0; i < nprobes; i++)
            {
                int ke = rnd.Next(nelements);
                total += id_index.GetAllByKey(ke).Count();
            }
            sw.Stop();
            Console.WriteLine($"GetAllByKey: {nprobes} probes ok. duration={sw.ElapsedMilliseconds} total={total}");

            sw.Restart();
            total = 0;
            rnd = new Random(7654331);
            for (int i = 0; i < nprobes; i++)
            {
                int ke = rnd.Next(nelements);
                total += name_index.SearchAll(new object[] { -1, "" + (ke), -1 }, 
                    //comp_like).Count();
                    comp).Count();
            }
            sw.Stop();
            Console.WriteLine($"SearchAll: {nprobes} probes ok. duration={sw.ElapsedMilliseconds} total={total}");

        }
    }
}
