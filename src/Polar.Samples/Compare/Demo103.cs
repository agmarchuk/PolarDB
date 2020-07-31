﻿using Polar.Common;
using Polar.DB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Samples.Compare
{
    public class Demo103 : ISample
    {
        public ICollection<IField> Fields { get {
                return new List<IField>() {
                    new NumericField("Number of elements", "nelements") { DefaultValue = 1_000_000 },
                    new NumericField("Number of probes", "nprobes") { DefaultValue = 1_000}
                };
            }  
        }

        private Stream stream;
        public void Clear()
        {
            try
            {
                if (stream != null)
                {
                    stream.Close();
                }
            }
            catch  { }
        }
        //START_SOURCE_CODE
        public void Run()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Random rnd = new Random();
            Console.WriteLine("Start Main18: bearing experiments");
            string dbpath = System.IO.Path.GetTempPath();
            int cnt = 0;
            Func<Stream> GenStream = () =>
            {
                stream = new FileStream(dbpath + "f" + (cnt++) + ".bin",
                FileMode.OpenOrCreate, FileAccess.ReadWrite);
                return stream;
            };
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
                        comp) { tmpdir = dbpath, volume_of_offset_array = 20_000_000 }
                };

            bool toload = true;

            if (toload)
            {
                sw.Restart();
                table.Clear();
                int n = nelements;
                table.Load(
                    Enumerable.Range(0, nelements)
                        .Select(i => new object[] { (n - i - 1), "" + (n - i - 1), 33 })
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
            var quer = name_index.SearchAll(new object[] { -1, "" + (key / 10), -1 }, comp_like);
            foreach (var ob in quer)
            {
                Console.WriteLine("~~" + tp_elem.Interpret(ob));
            }

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
                    comp_like).Count();
                //comp).Count();
            }
            sw.Stop();
            Console.WriteLine($"SearchAll: {nprobes} probes ok. duration={sw.ElapsedMilliseconds} total={total}");
            table.Close();
        }
        //END_SOURCE_CODE
        public string Name { get; set; }
        public string DiplayName { get => "Demo103"; }
        public int nelements;
        public int nprobes;
    }
}
