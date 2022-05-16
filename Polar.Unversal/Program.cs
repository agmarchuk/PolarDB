using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Polar.DB;

namespace Polar.Universal
{
    class Program
    {
        static void Main(string[] args)
        {
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            Console.WriteLine("Start Univeral!");
            string path = @"D:\Home\data\uni\";
            int nom = 0;
            Func<Stream> GenStream = () => File.Open(path +"f" + nom++ + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            USequence sequ = new USequence(
                new PTypeRecord(
                    new NamedType("id", new PType(PTypeEnumeration.integer)),
                    new NamedType("name", new PType(PTypeEnumeration.sstring)),
                    new NamedType("age", new PType(PTypeEnumeration.integer))),

                GenStream, 
                rec => false,
                rec => (int)((object[])rec)[0],
                id => (int)id);
            
            int nrecords = 100_000_000;
            
            bool toload = false;
            if (toload)
            {
                sw.Restart();
                sequ.Clear();
                sequ.Load(
                    Enumerable.Range(0, nrecords)
                    .Select(ii => new object[] { nrecords - ii - 1, "n" + (nrecords - ii - 1), 27 })
                    );
                sw.Stop();
                Console.WriteLine($"load ok. duration={sw.ElapsedMilliseconds}");
            }


            int key = nrecords / 3 * 2;
            object[] v = (object[])sequ.GetByKey(key);
            Console.WriteLine($"{v[0]} {v[1]} {v[2]} ");

            sw.Restart();
            int nprobes = 1000;
            for (int i=0; i<nprobes; i++)
            {
                key = rnd.Next(nrecords);
                v = (object[])sequ.GetByKey(key);
                if (v == null) Console.WriteLine($"null using key={key}");
                if ((int)v[0] != key)  Console.WriteLine($"{v[0]} {v[1]} {v[2]} ");
            }
            sw.Stop();
            Console.WriteLine($"{nprobes} gets ok. duration={sw.ElapsedMilliseconds}");

        }
    }
}
