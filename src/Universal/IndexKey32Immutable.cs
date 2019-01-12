using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Universal
{
    public class IndexKey32Immutable : IIndex //IIndexImmutable<Tkey> where Tkey : IComparable
    {
        //private bool keyisint = false;
        private UniversalSequenceBase key_arr;
        private UniversalSequenceBase off_arr;
        private Func<object, int> keyProducer;
        private Func<Stream> streamGen;
        public Sequence BearingSequence { get; set; }
        //public Func<object, long> offsetProducer { get; set; }
        //private PType tp_index_el;
        private Func<int, Diapason> GetDia = null;
        public IndexKey32Immutable(Func<Stream> streamGen, Func<object, int> keyProducer)
        {
            this.streamGen = streamGen;
            this.keyProducer = keyProducer;
            key_arr = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), streamGen());
            off_arr = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());
        }

        public void Clear() { key_arr.Clear(); off_arr.Clear(); }
        //public void AppendPosition(long offset, object element)
        //{
        //    index_arr.AppendElement(new object[] { keyProducer(element), offset });
        //}
        public void Flush()
        {
            key_arr.Flush(); off_arr.Flush();
        }
        public void Build()
        {
            int nelems = (int)BearingSequence.Count();
            int[] keys = new int[nelems];
            long[] offs = new long[nelems];
            int p = 0;
            BearingSequence.Scan((off, obj) =>
            {
                keys[p] = (int)keyProducer(obj);
                offs[p] = off;
                p++;
                return true;
            });
            Array.Sort(keys, offs);
            key_arr.Clear();
            for (p = 0; p < nelems; p++) key_arr.AppendElement(keys[p]);
            key_arr.Flush();
            off_arr.Clear();
            for (p = 0; p < nelems; p++) off_arr.AppendElement(offs[p]);
            off_arr.Flush();

            Prepare();
        }
        public void Prepare()
        {
            //if (index_arr.Count() < 100) return;
            //long N = index_arr.Count();
            //int min = keyProducer(index_arr.GetByIndex(0L));
            //int max = keyProducer(index_arr.GetByIndex(N - 1));
            //GetDia = Scale.GetDiaFunc32(index_arr.ElementValues().Select(ob => keyProducer(ob)), min, max, (int)(N / 16));
        }

        // ============================ Бинарные поиск ==============================

        public IEnumerable<long> BinarySearchAll(int key)
        {
            long start = 0L;
            long number = key_arr.Count();
            if (GetDia != null)
            {
                var dia = GetDia(key);
                start = dia.start;
                number = dia.numb;
            }
            return BinarySearchAll(start, number, key);
        }
        public IEnumerable<long> BinarySearchAll(long start, long numb, int key)
        {
            if (numb == 0) return Enumerable.Empty<long>();
            return BinarySearch(start, numb, key);
        }
        private const int maxsegmentsize = 100;
        // Ищет все решения внутри имея ввиду, что слева за диапазоном уровень меньше нуля, справа за диапазоном больше 
        private IEnumerable<long> BinarySearch(long start, long number, int key)
        {
            if (true)
            {
                long ll = start;
                int numb = (int)number;
                for (int i=0; i<numb; i++)
                {
                    int ke = (int)key_arr.GetByIndex(ll + i);
                    if (ke < key) continue;
                    else if (ke == key) yield return (long)off_arr.GetByIndex(ll+i);
                    else break;
                }
            }
            //else if (number < maxsegmentsize)
            //{
            //    foreach (var ob in index_arr.ElementValues(index_arr.ElementOffset(start), number))
            //    {
            //        int k = keyProducer(ob);
            //        if (k < key) continue;
            //        else if (k == key) yield return ob;
            //        else break;
            //    }
            //}
            //else
            //{
            //    long half = number / 2;
            //    object middle_obj = index_arr.GetByIndex(start + half);
            //    int middle_key = keyProducer(middle_obj);
            //    if (half > 0)
            //    {

            //        if (middle_key < key)
            //        {
            //            foreach (var ob in BinarySearch(start + half + 1, number - half - 1, key)) yield return ob;
            //        }
            //        else if (middle_key > key)
            //        {
            //            foreach (var ob in BinarySearch(start, half, key)) yield return ob;
            //        }
            //        else // Если равно
            //        {
            //            foreach (var ob in BinarySearch(start, half, key)) yield return ob;
            //            yield return middle_obj;
            //            foreach (var ob in BinarySearch(start + half + 1, number - half - 1, key)) yield return ob;
            //        }
            //    }
            //    else
            //    {
            //        if (middle_key == key) yield return middle_obj;
            //    }
            //}
        }

    }

}
