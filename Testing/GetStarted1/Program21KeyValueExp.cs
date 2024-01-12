using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    partial class Program
    {
        public static void Main21()
        {
            Console.WriteLine("Start Program21KeyValueExp");
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            string path = dbpath + "Databases/";
            int cnt = 0;
            Func<Stream> genStream = () => File.Open(path + "data" + (cnt++) + ".bin",
                FileMode.OpenOrCreate, FileAccess.ReadWrite);
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));
            KVStorage32 storage = new KVStorage32(tp_person, genStream);

            sw.Restart();
            int npersons = 1_000_000;
            storage.Build(Enumerable.Range(0, npersons)
                .Select(i => new object[] { npersons - i - 1, "Pupkin " + (npersons - i - 1), 33.0 }));
            sw.Stop();
            Console.WriteLine($"load duration={sw.ElapsedMilliseconds}");

            int key = npersons * 2 / 3;
            var qq = storage.GetAllByKey(key);
            foreach (object q in qq) 
                Console.WriteLine(tp_person.Interpret(q));

            int nprobe = 10_000;
            int total = 0;
            sw.Restart();
            for (int i=0; i<nprobe; i++)
            {
                int ke = rnd.Next(npersons);
                total += storage.GetAllByKey(key).Count();
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} probes ok. duration={sw.ElapsedMilliseconds}");
        }
    }
    public class KVStorage32
    {
        private PType tp;
        //BearingPure table;
        BearingDeletable table;
        IndexKey32Comp key_index;
        public KVStorage32(PType tp, Func<Stream> medias)
        {
            this.tp = tp;
            //table = new BearingPure(tp, medias);
            table = new BearingDeletable(tp, medias);
            key_index = new IndexKey32Comp(medias, table, ob => true, ob => (int)((object[])ob)[0], null);
            table.Indexes = new IIndex[] { key_index };
        }
        public void Build(IEnumerable<object> flow)
        {
            table.Clear();
            table.Load(flow);
        }
        public IEnumerable<object> GetAllByKey(int key)
        {
            var q = key_index.GetAllByKey(key);
            return q;
        }
    }
}
