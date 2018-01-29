using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    class TripleStore_mag
    {
        // Есть таблица имен для хранения строк IRI
        //private Mag_Nametable nametable; // пока не используется

        // Тип Object Variants
        PType tp_ov = new PTypeUnion(
            new NamedType("dummy", new PType(PTypeEnumeration.none)),
            new NamedType("iri", new PType(PTypeEnumeration.integer)),
            new NamedType("str", new PType(PTypeEnumeration.sstring)));
        // Тип триплепта
        PType tp_triple;

        // Основная таблица - таблица триплетов
        private UniversalSequenceBase table;
        // Индекс
        private UniversalSequenceCompKey32 index_spo;
        // Компаратор
        Comparer<object> spo_comparer;
        Func<object, int> keyFunc;
        public TripleStore_mag(Stream tab_stream, Stream spo_stream, Comparer<object> comp)
        {
            this.spo_comparer = comp;
            tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            table = new UniversalSequenceBase(tp_triple, tab_stream);
            spo_comparer = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                object[] aa = (object[])a; object[] bb = (object[])b;
                int cmp = ((int)aa[0]).CompareTo((int)bb[0]);
                return cmp;
            }));
            keyFunc = tri => (int)((object[])tri)[0];
            index_spo = new UniversalSequenceCompKey32(spo_stream, keyFunc, spo_comparer, table);
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

            object[] arr_triples = new object[nelements];
            // Выделяем группы одинаковых ключей и сортируем по компаратору
            int index = 0;
            int length = 0;
            int current_key = keys[index];
            for (int i=0; i<nelements; i++)
            {
                int key = keys[i];
                if (key == current_key)
                {
                    length++;
                }
                else
                {
                    LocalSort(arr_offs, arr_triples, index, length);
                    // начинается новый блок
                    index = i;
                    length = 1;
                }
            }
            LocalSort(arr_offs, arr_triples, index, length);

            // Записываем итог
            index_spo.Clear();
            for (int i = 0; i < nelements; i++) index_spo.AppendElement(new object[] { keys[i], arr_offs[i] });
            index_spo.Flush();
        }

        private void LocalSort(long[] arr_offs, object[] arr_triples, int index, int length)
        {
            // выделен блок, начало index, длина length, читаем по офсетам, пишем по местам
            for (int j = index; j < index + length; j++) arr_triples[j] = table.GetElement(arr_offs[j]);
            // сортируем
            Array.Sort(arr_triples, arr_offs, index, length, spo_comparer);
            // чистим память от объектов
            for (int j = index; j < index + length; j++) arr_triples[j] = null;
        }

        public void Look()
        {
            foreach (long offset in index_spo.ElementValues().Select(pair => ((object[])pair)[1]).Take(10))
            {
                var v = table.GetElement(offset);
                Console.Write($"{tp_triple.Interpret(v)} ");
            }
            Console.WriteLine();
            int nprobe = 1000;
            Random rnd = new Random();
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            //sw.Restart();
            //for (int i = 0; i < nprobe; i++)
            //{
            //    int subj = rnd.Next((int)(index_spo.Count() / 2));
            //    long offset = index_spo.BinarySearchOffsetAny(new object[] { subj, null, null });
            //    if (offset == long.MinValue) throw new Exception("not found");
            //    var v = table.GetElement(offset);
            //    //Console.WriteLine($"subj={subj} triple={tp_triple.Interpret(v)}");

            //}
            //sw.Stop();
            //Console.WriteLine($"{nprobe} GetAny search ok. duration={sw.ElapsedMilliseconds}");

            sw.Restart();
            for (int i = 0; i < nprobe; i++)
            {
                int subj = rnd.Next((int)(index_spo.Count() / 2));
                object sample = new object[] { subj, null, null };
                int key = keyFunc(sample);
                var res = index_spo.BinarySearchAll(0, index_spo.Count(), key, sample)
                    //.Select(off => table.GetElement(off))
                    ;
                if (res.Count() != 2)
                {
                    //var arr = res.ToArray();
                    //foreach (object ob in res)
                    //{
                    //    Console.WriteLine($"ok. {tp_triple.Interpret(ob)}");
                    //}
                    //Console.WriteLine();
                    //var offs = index_spo.BinarySearchAllInside(0, index_spo.Count(), new object[] { subj, null, null }).ToArray();
                    //foreach (var off in offs)
                    //{

                    //}
                    Console.WriteLine($"res.Count()={res.Count()}");
                }
            }
            sw.Stop();
            Console.WriteLine($"{nprobe} GetAll search ok. duration={sw.ElapsedMilliseconds}");
        }
        public IEnumerable<object> GetTriplesBySubj(int subj)
        {
            object sample = new object[] { subj, null, null };
            //index_spo;
            return Enumerable.Empty<object>();
        }
    }
}

