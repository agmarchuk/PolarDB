using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    partial class Program
    {
        public static void Main13()
        {
            string path = "../../../";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start GetStarted/Main13");
            int cnt = 0;
            TripleStore32 store = new TripleStore32(()=> new FileStream(path + "Databases/f"+(cnt++)+".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite), path);
            int nelements = 1_000_000;
            // Начало таблицы имен 0 - type, 1 - name, 2 - person
            int b = 3; // Начальный индекс назначаемых идентификаторов сущностей

            var query = Enumerable.Range(0, nelements)
                .SelectMany(i => new object[]
                {
                    new object[] { nelements + b - i - 1, 0, new object[] { 1, 2 } },
                    new object[] { nelements + b - i - 1, 1, new object[] { 2, "pupkin" + (nelements + b - i - 1) } }
                }); // по 2 триплета на запись

            bool toload = true;
            if (toload)
            {
                sw.Restart();
                store.Load(query);
                store.Build();
                sw.Stop();
                Console.WriteLine($"load of {nelements * 2} triples ok. Duration={sw.ElapsedMilliseconds}");

                sw.Restart();
                store.BuildTest();
                sw.Stop();
                Console.WriteLine($"Test ===== Sorting of {nelements * 2} triples ok. Duration={sw.ElapsedMilliseconds}");

            }
            else
            {
                //store.BuildScale();
                //sw.Stop();
                //Console.WriteLine($"build Scale for {nelements * 2} triples ok. Duration={sw.ElapsedMilliseconds}");
            }

            // Для проверки работы запрошу запись с ключом nelements * 2 / 3
            int ke = nelements * 2 / 3 + 2;
            var qu = store.GetBySubject(ke);
            foreach (object[] t in qu)
            {
                Console.WriteLine($"{t[0]} {t[1]}");
            }
            var qu1 = store.GetTest(ke);
            foreach (object[] t in qu)
            {
                Console.WriteLine($"test ====== {t[0]} {t[1]}");
            }



            int nprobe = 1000;
            Random rnd = new Random();

            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int subj = rnd.Next(nelements);
                var quer = store.GetBySubject(subj);
                if (quer.Count() != 2)
                {
                    foreach (object[] t in quer)
                    {
                        Console.WriteLine($"{t[0]} {t[1]}");
                    }
                }
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetAll search ok. duration={sw.ElapsedMilliseconds}");

            nprobe = 10000;
            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int subj = rnd.Next(nelements);
                var quer = store.GetTest(subj);
                if (quer.Count() != 2)
                {
                    foreach (object[] t in quer)
                    {
                        Console.WriteLine($"{t[0]} {t[1]}");
                    }
                }
            }
            sw.Stop();
            Console.WriteLine($"Test ========= {nprobe} GetAll search ok. duration={sw.ElapsedMilliseconds}");

        }

    }
    /// <summary>
    /// Класс организует хранилище (набор) триплетов на задаваемом множестве стримов. Стримы порождаются или воспроизводятся 
    /// через генератор. Триплет - это тройка субъект, предикат, объект. Субъекты и предикаты - целые, объект или целое или 
    /// строка. Хранилище можно очистить Clear(), загрузить триплетами Load(поток триплетов), приготовить Prepare(), если 
    /// уже триплеты загружены, а хранилище запускается, и методы использования (доступа).
    /// </summary>
    public class TripleStore32
    {
        // типы ObjectVariants и триплета
        private PType tp_ov;
        private PType tp_triple;
        // Основная таблица - таблица триплетов
        private UniversalSequenceBase table;
        // Индекс
        private UniversalSequenceCompKey32 index_spo;
        private Func<object, int> keyFunc;
        private Func<int, Diapason> scaleFunc;

        private IndexViewImm indexTest;
        public TripleStore32(Func<Stream> stream_gen, string tmp_dir)
        {
            // Тип Object Variants
            PType tp_ov = new PTypeUnion(
                new NamedType("dummy", new PType(PTypeEnumeration.none)),
                new NamedType("iri", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)));
            tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            table = new UniversalSequenceBase(tp_triple, stream_gen());
            var spo_comparer = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                object[] aa = (object[])a; object[] bb = (object[])b;
                int cmp = ((int)aa[0]).CompareTo((int)bb[0]);
                return cmp;
            }));
            keyFunc = tri => (int)((object[])tri)[0];
            index_spo = new UniversalSequenceCompKey32(stream_gen(), keyFunc, spo_comparer, table);
            indexTest = new IndexViewImm(stream_gen, table,
                Comparer<object>.Create(new Comparison<object>((object a, object b) =>
                {
                    object[] aa = (object[])a; object[] bb = (object[])b;
                    int cmp = ((int)aa[0]).CompareTo((int)bb[0]);
                    return cmp;
                })), tmp_dir, 20_000_000
                );
        }
        public void Load(IEnumerable<object> triples)
        {
            table.Clear();
            foreach (object tri in triples)
            {
                long off = table.AppendElement(tri);
                int key = keyFunc(tri);
                index_spo.AppendElement(new object[] { key, off });
            }
            table.Flush();
            index_spo.Flush();
        }
        public void Build()
        {
            int nelements = (int)table.Count();
            int[] keys = new int[nelements];
            long[] arr_offs = new long[nelements];
            for (int i = 0; i < nelements; i++)
            {
                long off = i == 0 ? 8L : table.ElementOffset();
                arr_offs[i] = off;
                object v = table.GetElement(off);
                //arr_triples[i] = v;
                keys[i] = keyFunc(v);
            }
            Array.Sort(keys, arr_offs);

            scaleFunc = Scale.GetDiaFunc32(keys);

            // Записываем итог
            index_spo.Clear();
            for (int i = 0; i < nelements; i++) index_spo.AppendElement(new object[] { keys[i], arr_offs[i] });
            index_spo.Flush();
        }
        public void BuildTest()
        { 
            // Другой ва тестирование
            indexTest.Build();
        }

        //private void LocalSort(long[] arr_offs, object[] arr_triples, int index, int length)
        //{
        //    // выделен блок, начало index, длина length, читаем по офсетам, пишем по местам
        //    for (int j = index; j < index + length; j++) arr_triples[j] = table.GetElement(arr_offs[j]);
        //    // сортируем
        //    Array.Sort(arr_triples, arr_offs, index, length, spo_comparer);
        //    // чистим память от объектов
        //    for (int j = index; j < index + length; j++) arr_triples[j] = null;
        //}

        public void BuildScale()
        {
            int nelements = (int)table.Count();
            int[] keys = new int[nelements];
            for (int i = 0; i < nelements; i++)
            {
                object[] pair = (object[])index_spo.GetByIndex(i);
                keys[i] = (int)pair[0];
            }

            scaleFunc = Scale.GetDiaFunc32(keys);
        }

        public IEnumerable<object> GetBySubject(int subj)
        {
            long start = 0;
            long number = index_spo.Count();
            var res = index_spo.BinarySearchAll(start, number, subj, new object[] { subj, null, null })
                .Select(off => table.GetElement(off))
                .ToArray()
            ;
            return res;
        }

        public IEnumerable<object> GetTest(int subj)
        {
            return indexTest.SearchAll(new object[] { subj, null, null });
        }

    }
}
