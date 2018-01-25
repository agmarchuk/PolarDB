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
        private Comparer<object> comp;
        private UniversalSequenceBase bearing;
        //PType tp_elem;
        public UniversalSequenceComp(PType tp_elem, Stream media, Comparer<object> comp, UniversalSequenceBase bearing_table) : base(tp_elem, media)
        {
            this.comp = comp;
            this.bearing = bearing_table;
        }

        /*
        
        /// <summary>
        /// Метод выборки всех триплетов, удовлетворяющих условию компаратора
        /// </summary>
        /// <param name=""></param>
        /// <returns></returns>
        public IEnumerable<object> BinarySearchAll(object sample)
        {
            return BinarySearchAll(0, this.Count(), sample);
        }
        public IEnumerable<object> BinarySearchAll(long start, long numb, object sample)
        {
            // Пропустим возможные проверки применимости
            if (numb > 0)
            {
                // Начальный элемент
                var offset = (long)this.GetElement(this.ElementOffset(start));
                var elementFrom = bearing.GetElement(offset);
                return BinarySearchInside(elementFrom, numb, sample);
            }
            return Enumerable.Empty<object>();
        }

        // Ищет все решения внутри имея ввиду, что слева за диапазоном уровень меньше нуля, справа за диапазоном больше 
        private IEnumerable<object> BinarySearchInside(object elementFrom, long number, object sample)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = this.elem_size;
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
        */

    }
}
