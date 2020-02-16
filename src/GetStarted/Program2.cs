using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polar.DB;
using Polar.Cells;

namespace GetStarted
{
    public partial class Program
    {
        public static void Main2()
        {
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start demo of table and simple indexes");
            // ============ Таблица и простые индексы к ней =============
            // Создадим ячейку с последовательностью записей. Заполним последовательность тестовами значениями. 
            PType tp_pers = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));
            Stream stream = new FileStream(dbpath + "recordsequence.pac", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            PaCell cell = new PaCell(new PTypeSequence(tp_pers), stream, false);
            // Простой индекс, это отсортированная последовательность пар: ключ-офсет, в данном случае, ключ - идентификатор, офсет - смещение записи в ячейке cell 
            PType tp_index = new PTypeSequence(new PTypeRecord(
                new NamedType("key", new PType(PTypeEnumeration.integer)),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))
                ));
            PaCell cell_index = new PaCell(tp_index, new FileStream(dbpath + "id_index.pac", FileMode.OpenOrCreate, FileAccess.ReadWrite), false);
            int nelements = 1000000;
            bool toload = true; // Загружать или нет новую базу данных
            if (toload)
            {
                sw.Restart();
                // Очистим ячейки последовательности и индекса 
                cell.Clear();
                cell_index.Clear();
                // Запишем пустую последовательность длиной n элементов
                cell.Fill(new object[0]);
                cell_index.Fill(new object[0]);
                for (int i = 0; i < nelements; i++)
                {
                    int id = nelements - i; // обратный порядок для того, чтобы сортировка была не тривиальной
                    string name = "=" +id.ToString() + "="; // лексикографический порядок будет отличаться от числового
                    var offset =  cell.Root.AppendElement(new object[] { id, name, rnd.NextDouble() * 100.0 });
                    cell_index.Root.AppendElement(new object[] { id, offset });
                }
                // обязательно надо зафиксировать добавленное
                cell.Flush();
                cell_index.Flush();
                // Теперь надо отсортировать индексный массив по ключу
                cell_index.Root.SortByKey<int>(ob => (int)((object[])ob)[0]);
                sw.Stop();
                Console.WriteLine("Load ok. duration for {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
            }
            // Указатель на нулевой элемент последовательности
            PaEntry entry0 = cell.Root.Element(0);

            // Немножко проверим
            int search_key = nelements * 2 / 3;
            var entry = cell_index.Root.BinarySearchFirst(ent => ((int)ent.Field(0).Get()).CompareTo(search_key));
            if (entry.IsEmpty) { Console.WriteLine("no entry for key {0} found!", search_key); }
            else
            {
                // По найденой в индексе записи с совпадающим значением ключа, получаем офсет
                long seq_off = (long)entry.Field(1).Get();
                entry0.offset = seq_off;
                Console.WriteLine("name for key {0} is {1}!", search_key, entry0.Field(1).Get());
            }

            // Засечем скорость выборок
            sw.Restart();
            for (int i=0; i<1000; i++)
            {
                search_key = rnd.Next(nelements) + 1;
                entry = cell_index.Root.BinarySearchFirst(ent => ((int)ent.Field(0).Get()).CompareTo(search_key));
                if (entry.IsEmpty) { Console.WriteLine("no entry for key {0} found!", search_key); }
                else
                {
                    // По найденой в индексе записи с совпадающим значением ключа, получаем офсет
                    long seq_off = (long)entry.Field(1).Get();
                    entry0.offset = seq_off;
                    string name = (string)entry0.Field(1).Get();
                }
            }
            sw.Stop();
            Console.WriteLine("Duration for 1000 search in {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
        }
    }
}
