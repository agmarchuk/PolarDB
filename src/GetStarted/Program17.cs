using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    public partial class Program
    {
        public static void Main17()
        {
            Console.WriteLine("Start Main17: Testing indexes");
            string path = "../../../";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Random rnd = new Random();
            int cnt = 0;
            Func<Stream> GenStream = () => new FileStream(path + "Databases/f" + (cnt++) + ".bin",
                FileMode.OpenOrCreate, FileAccess.ReadWrite);

            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("years", new PType(PTypeEnumeration.integer)));

            UniversalSequenceBase table = new UniversalSequenceBase(tp_person, GenStream());
            //IndexKey32CompImmutable id_index = new IndexKey32CompImmutable(GenStream, table, obj =>
            //    new int[] { (int)((object[])obj)[0] }, null);
            //IndexKey32CompImmutable str_index = new IndexKey32CompImmutable(GenStream, table, obj =>
            //    new int[] { Hashfunctions.HashRot13((string)((object[])obj)[1]) }, null);

            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                return string.Compare((string)((object[])a)[1], (string)((object[])b)[1]);
            }));
            //IndexKey32CompImmutable str_index = new IndexKey32CompImmutable(GenStream, table, obj =>
            //    new int[] { Hashfunctions.First4charsRu((string)((object[])obj)[1]) }, comp);

            IndexViewImm nameview_index = new IndexViewImm(GenStream, table, comp, path + "Databases/", 20_000_000);

            //IndexKey32Imm i_index;
            //IndexKey32CompImmutable name_index;

            int nelements = 150_000_000;
            bool toload = false;

            if (toload)
            {
                table.Clear();
                sw.Restart();
                for (int i = 0; i < nelements; i++)
                {
                    table.AppendElement(new object[] { i, "" + i, 33 });
                }
                table.Flush();
                //id_index.Build();
                //str_index.Build();
                nameview_index.Build();
                sw.Stop();
                Console.WriteLine($"Load ok. Duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                sw.Restart();
                table.Refresh();
                //id_index.Refresh();
                //str_index.Refresh();
                nameview_index.Refresh();
                sw.Stop();
                Console.WriteLine($"Refresh ok. Duration={sw.ElapsedMilliseconds}");
            }


            int key = nelements * 2 / 3;
            //var obs = id_index.GetAllByKey(key);
            //var obs = str_index.GetAllByKey(Hashfunctions.HashRot13(""+key));
            //var obs = str_index.GetAllBySample(new object[] { -1, ""+key, -2 });
            var obs = nameview_index.BinarySearchAll(new object[] { -1, "" + key, -2 });
            foreach (var ob in obs)
            {
                Console.WriteLine(tp_person.Interpret(ob));
            }

            int nprobe = 1000;
            sw.Restart();
            int total = 0;
            for (int i=0; i<nprobe; i++)
            {
                int k = rnd.Next(nelements);
                //var os = id_index.GetAllByKey(k);
                //var os = str_index.GetAllByKey(Hashfunctions.HashRot13("" + k)).Where(ob => (string)((object[])ob)[1] == "" + k); 
                //var os = str_index.GetAllBySample(new object[] { -1, "" + k, -2 });
                var os = nameview_index.BinarySearchAll(new object[] { -1, "" + k, -2 });

                total += os.Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} queries ok. Duration={sw.ElapsedMilliseconds} total = {total}");
        }
    }
}

