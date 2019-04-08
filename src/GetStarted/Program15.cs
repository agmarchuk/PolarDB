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
        public static void Main15()
        {
            string path = ""; //"../../../";
            Random rnd = new Random();
            int cnt = 0;
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start Main15");

            Func<Stream> streamGen = () => new FileStream(path + "Databases/g" + (cnt++) + ".bin",
                FileMode.OpenOrCreate, FileAccess.ReadWrite);
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("years", new PType(PTypeEnumeration.real)));
            Comparer<object> comp_string = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                var aa = (string)((object[])a)[1];
                var bb = (string)((object[])b)[1];
                return aa.CompareTo(bb);
            }));
            UniversalSequenceBase table = new UniversalSequenceBase(tp_person, streamGen());
            //IndexKey32CompImm id_index = new IndexKey32CompImm(streamGen, table, ob => (int)((object[])ob)[0], null);
            //IndexKey32CompImm namehash_index = new IndexKey32CompImm(streamGen, table, 
            //    ob => Hashfunctions.HashRot13((string)((object[])ob)[1]), null);
            //IndexKey32CompImm namehash_index_full = new IndexKey32CompImm(streamGen, table,
            //    ob => Hashfunctions.HashRot13((string)((object[])ob)[1]), comp_string);
            IndexViewImm nameview_index = new IndexViewImm(streamGen, table, comp_string, path + "Databases/", 50_000_000);

<<<<<<< HEAD
            int nelements = 1_000_000;
=======
            int nelements = 400_000_000;
            Console.WriteLine($"nelements={nelements}");
>>>>>>> 830ff795b2cc2702534fcd3c52ee28bfde97824a

            // Загрузка
            bool toload = true;
            if (toload)
            {
                sw.Restart();
                var dataflow = Enumerable.Range(0, nelements)
                    .Select(nom => new object[] { nelements - nom - 1, "" + (nelements - nom - 1),
                        rnd.NextDouble() * 100D });
                table.Clear();
                foreach (object[] element in dataflow) table.AppendElement(element);
                //id_index.Build();
                //namehash_index.Build();
                //namehash_index_full.Build();
                nameview_index.Build();
                sw.Stop();
                Console.WriteLine($"Indexes ok. duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                sw.Restart();
                table.Refresh();
                //id_index.Refresh();
                //namehash_index.Refresh();
                //namehash_index_full.Refresh();
                nameview_index.Refresh();
                sw.Stop();
                Console.WriteLine($"refresh {nelements} ok. duration={sw.ElapsedMilliseconds}");
            }

            int id = nelements * 2 / 3;
            int nprobe = 1000;
            int total = 0;
            //var query1 = id_index.GetAllBySample(new object[] { id, null, -1.0D });
            //foreach (var obj in query1) Console.WriteLine(tp_person.Interpret(obj));

            //sw.Restart();
            //for (int i=0; i< nprobe; i++)
            //{
            //    id = rnd.Next(nelements);
            //    total += id_index.GetAllBySample(new object[] { id, null, -1.0D }).Count();
            //}
            //sw.Stop();
            //Console.WriteLine($"GetById {nprobe} probes. duration={sw.ElapsedMilliseconds}");

            // Работаем с именами
            string name = "" + (nelements * 2 / 3);

            //var query2 = namehash_index.GetAllBySample(new object[] { -1, name, -1.0D });
            //foreach (var obj in query2) Console.WriteLine(tp_person.Interpret(obj));

            //total = 0;
            //sw.Restart();
            //for (int i = 0; i < nprobe; i++)
            //{
            //    name = "" + rnd.Next(nelements);
            //    total += namehash_index.GetAllBySample(new object[] { -1, name, -1.0D }).Count();
            //}
            //sw.Stop();
            //Console.WriteLine($"GetByNameHash {nprobe} probes. duration={sw.ElapsedMilliseconds} total={total}");

            // Работаем с именами и компаратором
            //name = "" + (nelements / 3);
            //var query3 = namehash_index_full.GetAllBySample(new object[] { -1, name, -1.0D });
            //foreach (var obj in query3) Console.WriteLine(tp_person.Interpret(obj));

            //total = 0;
            //sw.Restart();
            //for (int i = 0; i < nprobe; i++)
            //{
            //    name = "" + rnd.Next(nelements);
            //    total += namehash_index_full.GetAllBySample(new object[] { -1, name, -1.0D }).Count();
            //}
            //sw.Stop();
            //Console.WriteLine($"GetByNameHash Full {nprobe} probes. duration={sw.ElapsedMilliseconds} total={total}");

            // Работаем с view индексом
            name = "" + (nelements * 2 / 3);
            var query4 = nameview_index.BinarySearchAll(new object[] { -1, name, -1.0D });
            foreach (var obj in query4) Console.WriteLine(tp_person.Interpret(obj));

            nprobe = 1000;
            total = 0;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                name = "" + rnd.Next(nelements);
                total += nameview_index.BinarySearchAll(new object[] { -1, name, -1.0D }).Count();
            }
            sw.Stop();
            Console.WriteLine($"IndexView search for {nprobe} probes. duration={sw.ElapsedMilliseconds} total={total}");
        }
    }
}
