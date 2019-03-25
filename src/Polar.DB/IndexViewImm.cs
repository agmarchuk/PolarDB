using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class IndexViewImm : UniversalSequenceBase
    {
        private Func<Stream> streamGen;
        private UniversalSequenceBase bearing;
        private Comparer<object> comp;
        // создаем объект, подсоединяемся к носителям или создаем носители
        public IndexViewImm(Func<Stream> streamGen, UniversalSequenceBase bearing, Comparer<object> comp) :
            base(new PType(PTypeEnumeration.longinteger), streamGen())
        {
            this.streamGen = streamGen;
            this.bearing = bearing;
            this.comp = comp;
        }
        // Что нужно? Создать и использовать
        long[] offsets; // временное решение
        object[] elements; // --
        public void Build()
        {
            // формируем два массива
            int ne = (int)bearing.Count();
            //long[] offsets = new long[ne];
            offsets = new long[ne];
            //object[] elements = new object[ne];
            elements = new object[ne];
            int ind = 0;
            bearing.Scan((off, obj) => 
            {
                offsets[ind] = off;
                elements[ind] = obj;
                ind++;
                return true;
            });
            // Сортируем
            Array.Sort(elements, offsets, comp);


            this.Clear(); // очищаем
        }
        public IEnumerable<object> BinarySearchAll(object obj)
        {
            int pos = Array.BinarySearch(elements, obj, comp);
            if (pos == -1) return Enumerable.Empty<object>();
            while (pos - 1 >= 0 && comp.Compare(elements[pos - 1], obj) == 0) pos = pos - 1;
            var res = elements.Skip(pos).TakeWhile(ob => comp.Compare(ob, obj) == 0).ToArray();
            return res;
        }

    }
}
