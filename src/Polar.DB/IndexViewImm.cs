using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class IndexViewImm
    {
        private UniversalSequenceBase bearing;
        private UniversalSequenceBase offset_sequ;
        private Comparer<object> comp;
        // создаем объект, подсоединяемся к носителям или создаем носители
        public IndexViewImm(Stream stream, UniversalSequenceBase bearing, Comparer<object> comp)
        {
            this.bearing = bearing;
            this.comp = comp;
            offset_sequ = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), stream);
        }
        // Что нужно? Создать и использовать
        private object[] rare_elements = null; // --
        public void Build()
        {
            long[] offsets; // временное решение
            object[] elements; // --
            int ne = (int)bearing.Count();
            // формируем два массива
            offsets = new long[ne];
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

            // очищаем индексный массив
            offset_sequ.Clear();
            for (int i=0; i<ne; i++)
            {
                offset_sequ.AppendElement(offsets[i]);
            }
            offset_sequ.Flush();

            // Теперь оставим только часть массивов
            rare_elements = elements.Where((obj, i) => i % Nfactor == 0).ToArray();
            //offsets = offsets.Where((o, i) => i % Nfactor == 0).ToArray();

        }
        // Коэффициент прореживания массива elements, подбирался экспериментально. Лучшие по скорости результаты 16-20
        private int Nfactor = 40;
        public void Refresh() { }

        // Поиск в последовательностях
        private IEnumerable<object> BinarySearchAll(long start, long number, object sample)
        {
            long half = number / 2;
            if (half == 0)
            {
                // Получаем офсет, по нему получаем объект элемента
                long offse = (long)offset_sequ.GetByIndex(start);
                object obje = bearing.GetElement(offse);
                int cmp = comp.Compare(obje, sample);
                if (cmp == 0) return Enumerable.Repeat<object>(obje, 1);
                else return Enumerable.Empty<object>(); // Не найден
            }

            long middle = start + half;
            long rest = number - half - 1;
            //object[] mid_pair = (object[])keyoffsets.GetByIndex(middle);
            long middle_offse = (long)offset_sequ.GetByIndex(middle);
            object middle_obje = bearing.GetElement(middle_offse);
            //var middle_depth = comp.Compare(mid_pair, key, sample);
            var middle_depth = comp.Compare(middle_obje, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                IEnumerable<object> flow = BinarySearchAll(start, half, sample)
                    .Concat(Enumerable.Repeat<object>(middle_obje, 1));
                if (rest > 0) return flow.Concat(BinarySearchAll(middle + 1, rest, sample));
                else return flow;
            }
            if (middle_depth < 0)
            {
                if (rest > 0) return BinarySearchAll(middle + 1, rest, sample);
                else return Enumerable.Empty<object>();
            }
            else // middle_depth > 0
            {
                return BinarySearchAll(start, half, sample);
            }
        }

        // ================= Поиск по массиву elements в ОЗУ ==============
        public IEnumerable<object> BinarySearchAll(object obj)
        {
            long start = 0;
            long numb = offset_sequ.Count();
            if (rare_elements != null)
            {
                var dia = BSDia(0, rare_elements.Length, obj);
                start = dia.Item1 * Nfactor;
                numb = dia.Item2 * Nfactor;
            }
            var res = BinarySearchAll(start, numb, obj);
            return res;
        }

        /// <summary>
        /// Ищет (минимальный) диапазон в массиве elements такой что первая точка <=0, а следующая за последней 
        /// точка - точно больше нуля. Поскольку есть пропущенные, первая точка может быть =0 только если она имеет
        /// индекс 0. Если первая точка > 0, то диапазон пустой. 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="number"></param>
        /// <param name="sample"></param>
        /// <returns>диапазон start, number в массиве elements</returns>
        private (int, int) BSDia(int start, int number, object sample)
        {
            if (number == 0) return (start, 0);
            if (comp.Compare(rare_elements[start], sample) > 0) return (start, 0);
            if (number == 1) return (start, number);

            int half = number / 2;
            int middle = start + half;
            int rest = number - half - 1;
            object middle_obje = rare_elements[middle];
            var middle_depth = comp.Compare(middle_obje, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                int sta, num;
                var left = BSDia(start, half, sample);
                if (left.Item2 == 0) { sta = half; num = 1; }
                else { sta = left.Item1; num = left.Item2 + 1; }
                if (rest > 0) { var right = BSDia(middle + 1, rest, sample); num += right.Item2; }
                return (sta, num);
            }
            if (middle_depth < 0)
            {
                if (rest == 0) return (middle, 1);
                var d = BSDia(middle + 1, rest, sample);
                if (d.Item2 == 0) return (middle, 1);
                return d;
            }
            else // middle_depth > 0
            {
                return BSDia(start, half, sample);
            }
        }

    }
}
