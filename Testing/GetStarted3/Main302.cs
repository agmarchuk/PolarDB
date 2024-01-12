using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;

using Polar.DB;

namespace GetStarted3
{
    partial class Program
    {
        public static void Main302()
        {
            Console.WriteLine("Start Main302");
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
            int nelements = 10_000_000;
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

            sw.Restart();
            long sum = 0;
            foreach (object[] r in sequence.ElementValues())
            {
                sum += (int)r[2];
            }
            sw.Stop();
            Console.WriteLine($"scan of {nelements} duration {sw.ElapsedMilliseconds} ms. sum={sum}");

            sw.Restart();
            sum = 0;
            foreach (object[] r in Enumerable.Range(0, nelements).Select(i => new object[] { i, "Иванов" + i, 55 }))
            {
                sum += (int)r[2];
            }
            sw.Stop();
            Console.WriteLine($"object flow generation of {nelements} duration {sw.ElapsedMilliseconds} ms. sum={sum}");

            sw.Restart();
            sum = 0;
            foreach (var r in Enumerable.Range(0, nelements).Select(i => new AAA() { id=i, name="Иванов" + i, age = rnd.Next(1, 110) }))
            {
                sum += (int)r.age;
            }
            sw.Stop();
            Console.WriteLine($"struct flow generation of {nelements} duration {sw.ElapsedMilliseconds} ms. sum={sum}");

            sw.Restart();
            sum = 0;
            foreach (var r in Enumerable.Range(0, nelements).Select(i => new Tuple<int, string, int>(i, "Иванов" + i, rnd.Next(1, 110))))
            {
                sum += r.Item3;
            }
            sw.Stop();
            Console.WriteLine($"tuple flow generation of {nelements} duration {sw.ElapsedMilliseconds} ms. sum={sum}");

            sw.Restart();
            sum = 0;
            foreach (var r in Enumerable.Range(0, nelements).Select(i => new Tuple<int, long, int>(i, (long)i, rnd.Next(1, 110))))
            {
                sum += r.Item3;
            }
            sw.Stop();
            Console.WriteLine($"allint tuple flow generation of {nelements} duration {sw.ElapsedMilliseconds} ms. sum={sum}");

            sw.Restart();
            sum = 0;
            foreach (var r in Enumerable.Range(0, nelements).Select(i => new object[] { i, (long)i, rnd.Next(1, 110) }))
            {
                sum += (int)r[2];
            }
            sw.Stop();
            Console.WriteLine($"allint object flow generation of {nelements} duration {sw.ElapsedMilliseconds} ms. sum={sum}");

            sw.Restart();
            sum = 0;
            foreach (var r in Enumerable.Range(0, nelements).Select(i => 55))
            {
                sum += (int)r;
            }
            sw.Stop();
            Console.WriteLine($"int flow generation of {nelements} duration {sw.ElapsedMilliseconds} ms. sum={sum}");

            //scan of 10000000 duration 2133 ms.sum = 549806436
            //object flow generation of 10000000 duration 858 ms.sum = 550000000
            //struct flow generation of 10000000 duration 810 ms.sum=550058422
            //tuple flow generation of 10000000 duration 837 ms.sum=549922381
            //allint tuple flow generation of 10000000 duration 279 ms.sum=550001434
            //allint object flow generation of 10000000 duration 501 ms.sum=549901363
            //int flow generation of 10000000 duration 118 ms.sum=550000000
            return;

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
    struct AAA { public int id, age; public string name; }
    struct BBB { public int id, age; public long offset; }
}
