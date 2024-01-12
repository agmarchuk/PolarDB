using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Polar.DB;

namespace GetStarted4
{
    partial class Program
    {
        public static void Main402()
        {
            Console.WriteLine("Start Main402");
            // Создадим типы записи и последовательности записей
            PType tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            PType tp_seq = new PTypeSequence(tp_rec);

            // ======== Универсальная последовательность ==========
            Stream stream = File.Open(datadirectory_path + "first_data.bin", FileMode.OpenOrCreate);
            UniversalSequenceBase sequence = new UniversalSequenceBase(tp_rec, stream);

            Random rnd = new Random();
            int nelements = 100_000;
            // Последовательность создается пустой или она может быть очищена
            sequence.Clear();
            // В последовательность можно добавлять элементы в объектном представлении
            for (int i = 0; i < nelements; i++)
            {
                sequence.AppendElement(new object[] { i, "Иванов" + i, rnd.Next(1, 110) });
            }

            // Серию записей обязательно надо завершать сбросом буферов
            sequence.Flush();

            // Изучим полученную последовательность
            Console.WriteLine($"Count={sequence.Count()}");
            foreach (object[] r in sequence.ElementValues().Skip(50_000).Take(20))
            {
                Console.WriteLine($"{r[0]} {r[1]} {r[2]} ");
            }

            // При заполнении массива, сохраним офсеты элементов в массиве
            long[] offsets = new long[nelements];
            int[] keys = new int[nelements];
            sequence.Clear();
            for (int i = 0; i < nelements; i++)
            {
                int key = nelements - i - 1;
                offsets[i] = sequence.AppendElement(new object[] {key , "Иванов" + key, rnd.Next(1, 110) });
                keys[i] = key;
            }

            // отсортируем пару массивов keys, offsets по ключам
            Array.Sort(keys, offsets);

            // Будем делать выборку элементов по ключу
            sw.Restart();
            int ntests = 10_000;
            for (int j=0; j<ntests; j++)
            {
                int key = rnd.Next(nelements);
                int ind = Array.BinarySearch(keys, key);
                long off = offsets[ind];
                object[] fields = (object[])sequence.GetElement(off);
                //Console.WriteLine($"key={key} {fields[0]} {fields[1]} {fields[2]}");
            }
            sw.Stop();
            Console.WriteLine($"duration of {ntests} tests is {sw.ElapsedMilliseconds} ms.");
        }
    }
}
