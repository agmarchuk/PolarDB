using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;


namespace Polar.DB
{
    /// <summary>
    /// Универсальная последовательность, имеющая целый полуключ и компаратор. В рабочем состоянии, каждому элементу опорной (bearing) 
    /// последовательности сопоставляется поляровская запись ключ-офсет и эти элементы отсортированы по компаратору. Ключ создается 
    /// из объектного представления элемента последовательности примерением ключевой функции и является основной частью сравнения. 
    /// Компаратор сравнивает объекты при совпадении ключа.
    /// </summary>
    public class UniversalSequenceCompKey32 : UniversalSequenceComp
    {
        private Func<object, int> keyfunc;
        public UniversalSequenceCompKey32(Stream media, Func<object, int> keyfunc, Comparer<object> comp, 
            UniversalSequenceBase bearing_table) : 
            base(new PTypeRecord(
                new NamedType("key", new PType(PTypeEnumeration.integer)),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))), media, comp, bearing_table)
        {
            this.keyfunc = keyfunc; 
        }

        public IEnumerable<long> BinarySearchAll(long start, long number, int key, object sample)
        {
            if (number < 40)
            {
                return this.ElementValues(ElementOffset(start), number)
                    .Where(pair => DoubleComp((object[])pair, key, sample) == 0)
                    .Select(pair => (long)((object[])pair)[1])
                    ;
            }
            long half = number / 2;
            if (half == 0)
            {
                // Получаем пару (ключ-офсет)
                object[] pair = (object[])GetByIndex(start);
                int cmp = DoubleComp(pair, key, sample);
                if (cmp == 0) return Enumerable.Repeat<long>((long)pair[1], 1);
                else return Enumerable.Empty<long>(); // Не найден
            }

            long middle = start + half;
            long rest = number - half - 1;
            object[] mid_pair = (object[])GetByIndex(middle);
            var middle_depth = DoubleComp(mid_pair, key, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                IEnumerable<long> flow = BinarySearchAll(start, half, key, sample).Concat(Enumerable.Repeat<long>((long)mid_pair[1], 1));
                if (rest > 0) return flow.Concat(BinarySearchAll(middle + 1, rest, key, sample));
                else return flow;
            }
            if (middle_depth < 0)
            {
                if (rest > 0) return BinarySearchAll(middle + 1, rest, key, sample);
                else return Enumerable.Empty<long>();
            }
            else
            {
                return BinarySearchAll(start, half, key, sample);
            }
        }

        private int DoubleComp(object[] pair, int key, object sample)
        {
            int k = (int)pair[0];
            int cmp = k.CompareTo(key);
            if (cmp == 0)
            {
                long o = (long)pair[1];
                cmp = comp.Compare(bearing.GetElement(o), sample);
            }
            return cmp;
        }
    }
    //class DoubleComparer32 : Comparer<(int, object)>
    //{
    //    private Comparer<object> comp;
    //    public DoubleComparer32(Comparer<object> comp)
    //    {
    //        this.comp = comp;
    //    }
    //    public override int Compare((int, object) o1, (int, object) o2)
    //    {
    //        int cmp = o1.Item1.CompareTo(o2.Item1);
    //        if (cmp != 0) return cmp;
    //        return comp.Compare(o1.Item2, o2.Item2);
    //    }
    //}
}
