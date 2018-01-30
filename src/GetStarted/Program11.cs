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
        public static void Main11()
        {
            Console.WriteLine("Start GetStarted/Main11");
            using (Stream tab_stream = File.Open(path + "tab_stream.bin", FileMode.OpenOrCreate))
            using (Stream spo_stream = File.Open(path + "spo_stream.bin", FileMode.OpenOrCreate))
            {
                TripleStore_mag store = new TripleStore_mag(tab_stream, spo_stream, null);

                int ntiples = 1_000_000;
                // Начало таблицы имен 0 - type, 1 - name, 2 - person
                int b = 3; // Начальный индекс назначаемых идентификаторов сущностей

                sw.Restart();
                var query = Enumerable.Range(0, ntiples)
                    .SelectMany(i => new object[]
                    {
                    new object[] { ntiples + b - i - 1, 0, new object[] { 1, 2 } },
                    new object[] { ntiples + b - i - 1, 1, new object[] { 2, "pupkin" + (ntiples + b - i - 1) } }
                    }); // по 2 триплета на запись
                store.Load(query);
                store.Build();
                sw.Stop();
                Console.WriteLine($"load of {ntiples * 2} triples ok. Duration={sw.ElapsedMilliseconds}");
                store.Look();
            }
        }
    }

}
