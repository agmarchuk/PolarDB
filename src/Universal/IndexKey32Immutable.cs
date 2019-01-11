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
        private bool keyisint = false;
        private UniversalSequenceBase index_arr;
        private Func<object, int> keyProducer;
        private Func<Stream> streamGen;
        public Sequence BearingSequence { get; set; }
        public Func<object, long> offsetProducer { get; set; }
        private PType tp_index_el;
        public IndexKey32Immutable(Func<object, int> keyProducer, Func<Stream> streamGen)
        {
            this.keyProducer = keyProducer;
            this.streamGen = streamGen;
            keyisint = true;
            PType tp_key = new PType(PTypeEnumeration.integer);
            this.tp_index_el = new PTypeRecord(
                new NamedType("key", tp_key),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger)));
            index_arr = new UniversalSequenceBase(tp_index_el, streamGen());
        }

        public void Clear() => index_arr.Clear();
        public void AppendPosition(long offset, object element)
        {
            index_arr.AppendElement(new object[] { keyProducer(element), offset });
        }
        public void Flush()
        {
            index_arr.Flush();
        }
        public void Build()
        {
            index_arr.Sort32(ob => { var k = keyProducer(ob); return (int)k; });
        }

        public IEnumerable<object> GetAllByKey(int key)
        {
            return Enumerable.Empty<object>();
        }

        // ============================ Бинарные поиск ==============================

        public IEnumerable<object> BinarySearchAll(int key)
        {
            //foreach (var qu in index_arr.ElementValues())
            //{
            //    Console.WriteLine(tp_index_el.Interpret(qu));
            //}
            return BinarySearchAll(0, index_arr.Count(), key);
        }
        public IEnumerable<object> BinarySearchAll(long start, long numb, int key)
        {
            if (numb == 0) return Enumerable.Empty<object>();
            return BinarySearch(start, numb, key);
        }
        private const int maxsegmentsize = 100;
        // Ищет все решения внутри имея ввиду, что слева за диапазоном уровень меньше нуля, справа за диапазоном больше 
        private IEnumerable<object> BinarySearch(long start, long number, int key)
        {
            if (number < maxsegmentsize)
            {
                foreach (var ob in index_arr.ElementValues(index_arr.ElementOffset(start), number))
                {
                    int k = keyProducer(ob);
                    if (k < key) continue;
                    else if (k == key) yield return ob;
                    else break;
                }
            }
            else
            {
                long half = number / 2;
                object middle_obj = index_arr.GetByIndex(start + half);
                int middle_key = keyProducer(middle_obj);
                if (half > 0)
                {

                    if (middle_key < key)
                    {
                        foreach (var ob in BinarySearch(start + half + 1, number - half - 1, key)) yield return ob;
                    }
                    else if (middle_key > key)
                    {
                        foreach (var ob in BinarySearch(start, half, key)) yield return ob;
                    }
                    else // Если равно
                    {
                        foreach (var ob in BinarySearch(start, half, key)) yield return ob;
                        yield return middle_obj;
                        foreach (var ob in BinarySearch(start + half + 1, number - half - 1, key)) yield return ob;
                    }
                }
                else
                {
                    if (middle_key == key) yield return middle_obj;
                }
            }
        }

    }

}
