﻿using Polar.Common;
using Polar.DB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Samples
{
    public class Demo102 : ISample
    {
        public ICollection<IField> Fields
        {
            get
            {
                return new List<IField>() {
                    new NumericField("Number of persons", "npersons") { DefaultValue = 1_000_000 },
                    new NumericField("Number of probes", "nprobe") { DefaultValue = 10_000}
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
            catch { }
        }

        //START_SOURCE_CODE
        public void Run()
        {
            Console.WriteLine("Start Demo201: work with bearing sequence");
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            // Определим тип
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            // Нам поднадобится несколько стримов, соорудим генератор
            string dbpath = System.IO.Path.GetTempPath();
            int istream = 0;
            Func<Stream> GenStream = () =>
            {
                Stream fs = File.Open(dbpath + "d102_" + istream + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);
                stream = fs;
                istream++;
                return fs;
            };
            // Создадим опорную последовательность или присоединимся к ней
            BearingDeletable table = new BearingDeletable(tp_person, GenStream);
            // Создадим ключевой индекс
            IndexKey32Comp index_id = new IndexKey32Comp(GenStream, table,
                ob => true,
                ob => (int)((object[])ob)[0],
                null);
            // Присоединим индекс к таблице
            table.Indexes = new IIndex[] { index_id };
            // Создадим генератор данных, данными будут персоны, а нумерация - по убыванию 
            Random rnd = new Random();
            Func<int, IEnumerable<object>> GenPers = nper => Enumerable.Range(0, nper)
                .Select(i => new object[] { nper - i - 1, "_" + (nper - i - 1), rnd.Next(130) });
            // Будут ветви загрузки и присоединения
            bool toload = true;

            sw.Restart();
            if (toload)
            {
                table.Clear();
                table.Load(GenPers(npersons));
                table.Build();
            }
            else // присоединяемся к имеющейся
            {
                table.Refresh();
            }
            sw.Stop();
            Console.WriteLine($"load/refresh ok. duration={sw.ElapsedMilliseconds}");

            // Надем элемент по коду
            int code = npersons * 2 / 3;
            var results = index_id.GetAllByKey(code);
            foreach (object res in results)
            {
                Console.WriteLine(tp_person.Interpret(res));
            }

            // Измерим скорость выборок
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int c = rnd.Next(npersons);
                var result = index_id.GetAllByKey(c).FirstOrDefault();
                if (result == null) throw new Exception("Err: 18371");
            }

            sw.Stop();
            Console.WriteLine($"{nprobe} GetAllByKey ok. duration={sw.ElapsedMilliseconds}");

            // Результаты: для 1 млн. персон загрузка 1260 мс., выборки 120 мс/10000, refresh 237 мс. (повторно 7 мс.)
        }
        //END_SOURCE_CODE
        public string Name { get; set; }
        public string DiplayName { get => "Demo102"; }
        public int npersons;
        public int nprobe;
    }
}
