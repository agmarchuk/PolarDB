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
        public static void Main305()
        {
            Console.WriteLine("Start Main305");
            // Создадим типы записи и последовательности записей
            PType tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            PType tp_seq = new PTypeSequence(tp_rec);

            // ======== Универсальная последовательность ==========
            Stream stream = File.Open(datadirectory_path + "data305.bin", FileMode.OpenOrCreate);
            UniversalSequenceBase sequence = new UniversalSequenceBase(tp_rec, stream);

            Random rnd = new Random();
            int nelements = 50_000;

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
            int ntests = 10000;
            for (int j = 0; j < ntests; j++)
            {
                int key = rnd.Next(nelements);
                int nom = Array.BinarySearch(keys, key);
                long nom1 = BinarySearchFirst(0, nelements, key, keys);
                if (nom1 != (long)nom) throw new Exception();
                long off = offsets[nom1];
                object[] fields = (object[])sequence.GetElement(off);
                if (key != (int)fields[0]) throw new Exception("1233eddf");
                //Console.WriteLine($"key={key} {fields[0]} {fields[1]} {fields[2]}");
            }
            sw.Stop();
            Console.WriteLine($"duration of {ntests} tests is {sw.ElapsedMilliseconds} ms.");
        }


        private static long BinarySearchFirst(long start, long number, int key, int[] arr)
        {
            long half = number / 2;
            if (half == 0) // number = 0 или 1
            {
                if (arr[start] == key) return start;
                else if (arr[start + 1] == key) return start + 1;
                else return -1;
            }

            long middle = start + half;
            long rest = number - half - 1;
            var middle_depth = arr[middle] - key;

            if (middle_depth == 0) // Нашли!
            {
                return middle;
            }
            if (middle_depth < 0)
            {
                return BinarySearchFirst(middle + 1, rest, key, arr);
            }
            else
            {
                return BinarySearchFirst(start, half, key, arr);
            }
        }

    }
}
