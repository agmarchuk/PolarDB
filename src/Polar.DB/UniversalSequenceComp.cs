using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
//using Polar.DB;

namespace Polar.DB
{
    /// <summary>
    /// Класс предоставляет индекс, организованный на элементах типа tp_elem, опирающийся на процедуру сравнения объектных представлений элементов
    /// </summary>
    public class UniversalSequenceComp : UniversalSequenceBase
    {
        protected Comparer<object> comp;
        protected UniversalSequenceBase bearing;
        //PType tp_elem;
        public UniversalSequenceComp(PType tp_elem, Stream media, Comparer<object> comp, UniversalSequenceBase bearing_table) : base(tp_elem, media)
        {
            this.comp = comp;
            this.bearing = bearing_table;
        }

        // Сначала реализуем более простой поиск - любого
        public long BinarySearchOffsetAny(object sample)
        {
            long off = (long)GetByIndex(0);
            int depth = comp.Compare(bearing.GetElement(off), sample);
            if (depth == 0) return off;
            if (depth > 0) return long.MinValue;
            return BinarySearchOffsetAny(0, Count(), sample);
        }
        // Элемент elementFrom уже проверенный и меньше 0
        private long BinarySearchOffsetAny(long elementFrom, long number, object sample)
        {
            long half = number / 2;
            if (half == 0) return long.MinValue; // Не найден
            //var factor = elementFrom.Type.HeadSize;
            long middle = elementFrom + half;
            long mid_offset = (long)GetByIndex(middle);
            var middle_depth = comp.Compare(bearing.GetElement(mid_offset), sample);

            if (middle_depth == 0) return mid_offset;
            if (middle_depth < 0)
            {
                return BinarySearchOffsetAny(middle, number - half, sample);
            }
            else
            {
                return BinarySearchOffsetAny(elementFrom, half, sample);
            }
        }
        // Теперь по аналогии, сделаем поиск всех
        /// <summary>
        /// Выдает офсеты всех элементов опорной таблицы, удовлетворяющих условию выборки, начальная точка не проверена
        /// </summary>
        /// <param name="start"></param>
        /// <param name="number"></param>
        /// <param name="sample"></param>
        /// <returns></returns>
        public IEnumerable<long> BinarySearchAllInside(long start, long number, object sample)
        {
            long half = number / 2;
            if (half == 0)
            {
                long moffset = (long)GetByIndex(start);
                if (comp.Compare(bearing.GetElement(moffset), sample) == 0) return Enumerable.Repeat<long>(moffset, 1);
                else return Enumerable.Empty<long>(); // Не найден
            }
                
            long middle = start + half;
            long mid_offset = (long)GetByIndex(middle);
            long rest = number - half - 1;
            var middle_depth = comp.Compare(bearing.GetElement(mid_offset), sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                IEnumerable<long> flow = BinarySearchAllInside(start, half, sample).Concat(Enumerable.Repeat<long>(mid_offset, 1));
                if (rest > 0) return flow.Concat(BinarySearchAllInside(middle + 1, rest, sample));
                else return flow;
            }
            if (middle_depth < 0)
            {
                if (rest > 0) return BinarySearchAllInside(middle + 1, rest, sample);
                else return Enumerable.Empty<long>();
            }
            else
            {
                return BinarySearchAllInside(start, half, sample);
            }
        }

        //private IEnumerable<long> BinarySearchAllLeft(long start, long number, object sample)
        //{
        //    return Enumerable.Empty<long>();
        //}

        //private IEnumerable<long> BinarySearchAllRight(long start, long number, object sample)
        //{
        //    return Enumerable.Empty<long>();
        //}

    }
}
