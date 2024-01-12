using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Polar.DB;

namespace GetStarted3
{
    partial class Program
    {
        public static void Main303()
        {
            Console.WriteLine("Start Main303");
            // Создадим типы записи и последовательности записей
            PType tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            PType tp_seq = new PTypeSequence(tp_rec);

            // ======== Универсальная последовательность ==========
            Stream stream = File.Open(datadirectory_path + "data303.bin", FileMode.OpenOrCreate);
            UniversalSequenceBase sequence = new UniversalSequenceBase(tp_rec, stream);

            Random rnd = new Random();
            int nelements = 10_000_000;

            // При заполнении массива, сохраним офсеты элементов в массиве
            long[] offsets = new long[nelements];
            int[] keys = new int[nelements];

            bool toload = true;

            if (toload)
            {
                sw.Restart();
                sequence.Clear();
                for (int i = 0; i < nelements; i++)
                {
                    int key = nelements - i - 1;
                    offsets[i] = sequence.AppendElement(new object[] { key, "Иванов" + key, rnd.Next(1, 110) });
                    keys[i] = key;
                }
                // отсортируем пару массивов keys, offsets по ключам
                Array.Sort(keys, offsets);
                sw.Stop();
                Console.WriteLine($"Load of {nelements} elements. duration={sw.ElapsedMilliseconds}");
            }
            else
            {
                int ind = 0;
                sequence.Scan((off, obj) =>
                {
                    offsets[ind] = off;
                    keys[ind] = (int)((object[])obj)[0];
                    ind++;
                    return true;
                });
                // отсортируем пару массивов keys, offsets по ключам
                Array.Sort(keys, offsets);
            }




            // Будем делать выборку элементов по ключу
            sw.Restart();
            int ntests = 1000;
            for (int j = 0; j < ntests; j++)
            {
                int key = rnd.Next(nelements);
                int nom = Array.BinarySearch(keys, key);
                long off = offsets[nom];
                object[] fields = (object[])sequence.GetElement(off);
                if (key != (int)fields[0]) throw new Exception("1233eddf");
                //Console.WriteLine($"key={key} {fields[0]} {fields[1]} {fields[2]}");
            }
            sw.Stop();
            Console.WriteLine($"duration of {ntests} tests is {sw.ElapsedMilliseconds} ms.");
        }

        // Результаты прогонов
        // Домшний desktop i3, 8 Gb RAM
        // 1 млн. записей. Загрузка 0.4 сек. 1000 тестов 6.8 мс.
        // 10 млн. записей. Загрузка 3.6 сек. 1000 тестов 7.5 мс.
        // 20 млн. записей. Загрузка 7.3 сек. 1000 тестов 7.7 мс.
        // 50 млн. записей. Загрузка 19 сек. 1000 тестов 5.2 с.
        // 100 млн. записей. Загрузка 42 сек. 1000 тестов 10.8 с.
    }
}
