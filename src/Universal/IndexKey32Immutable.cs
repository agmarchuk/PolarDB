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
        public IndexKey32Immutable(Func<object, int> keyProducer, Func<Stream> streamGen)
        {
            this.keyProducer = keyProducer;
            this.streamGen = streamGen;
            keyisint = true;
            PType tp_key = new PType(PTypeEnumeration.integer);
            PType tp_index_el = new PTypeRecord(
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
            return BinarySearchAll(0, index_arr.Count(), key);
        }
        public IEnumerable<object> BinarySearchAll(long start, long numb, int key)
        {
            if (numb == 0) return Enumerable.Empty<object>();
            object elementFrom = index_arr.GetByIndex(start); // первое значение в сегменте start, start+numb-1
            //if (llen > 0)
            //{
            //    var elementFrom = sequ.Element(start);
            //    //foreach (var pe in BinarySearchInside(elementFrom, llen, elementDepth)) yield return pe;
            //    return BinarySearchInside(elementFrom, llen, elementDepth);
            //}
            //return Enumerable.Empty<PaEntry>();
            return BinarySearchInside(elementFrom, start, numb, key);
        }
        // Ищет все решения внутри имея ввиду, что слева за диапазоном уровень меньше нуля, справа за диапазоном больше 
        private static IEnumerable<object> BinarySearchInside(object elementFrom, long start, long number, int key)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.Type.HeadSize;
                PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
                PaEntry aftermiddle = new PaEntry(elementFrom.Type, middle.offset + size, elementFrom.cell);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    foreach (var pe in BinarySearchLeft(elementFrom, half, elementDepth)) yield return pe;
                    yield return middle;
                    foreach (var pe in BinarySearchRight(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else if (middle_depth < 0)
                {
                    foreach (var pe in BinarySearchInside(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else // if (middle_depth > 0)
                {
                    foreach (var pe in BinarySearchInside(elementFrom, half, elementDepth)) yield return pe;
                }
            }
            else if (number == 1) // && half == 0) - возможно одно решение или их нет
            {
                if (elementDepth(elementFrom) == 0) yield return elementFrom;
            }
        }
        // Ищет все решения имея ввиду, что справа решения есть 
        private static IEnumerable<PaEntry> BinarySearchLeft(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.Type.HeadSize;
                PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
                PaEntry aftermiddle = new PaEntry(elementFrom.Type, middle.offset + size, elementFrom.cell);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    foreach (var pe in BinarySearchLeft(elementFrom, half, elementDepth)) yield return pe;
                    yield return middle;
                    // Переписать все из второй половины
                    for (long ii = 0; ii < number - half - 1; ii++)
                    {
                        yield return aftermiddle;
                        aftermiddle = new PaEntry(elementFrom.Type, aftermiddle.offset + size, elementFrom.cell);
                    }
                }
                else if (middle_depth < 0)
                {
                    foreach (var pe in BinarySearchLeft(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else throw new Exception("Assert err: 9283");
            }
            else if (number == 1) // возможно одно решение или их нет
            {
                if (elementDepth(elementFrom) == 0) yield return elementFrom;
            }
        }
        // Ищет все решения имея ввиду, что слева решения есть 
        private static IEnumerable<PaEntry> BinarySearchRight(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.Type.HeadSize;
                PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
                PaEntry aftermiddle = new PaEntry(elementFrom.Type, middle.offset + size, elementFrom.cell);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    // Переписать все из первой половины
                    PaEntry ef = elementFrom;
                    for (long ii = 0; ii < half; ii++)
                    {
                        yield return ef;
                        ef = new PaEntry(elementFrom.Type, ef.offset + size, elementFrom.cell);
                    }
                    yield return middle;
                    foreach (var pe in BinarySearchRight(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else if (middle_depth > 0)
                {
                    foreach (var pe in BinarySearchRight(elementFrom, half, elementDepth)) yield return pe;
                }
            }
            else if (number == 1) // возможно одно решение или их нет
            {
                if (elementDepth(elementFrom) == 0) yield return elementFrom;
            }
        }

    }

}
