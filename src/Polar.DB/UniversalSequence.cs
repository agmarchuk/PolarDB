using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
//using Polar.DB;

namespace Polar.DB
{
    /// <summary>
    /// Класс предоставляет последовательность элементов типа tp_elem, опирающуюся на индекс или полуиндекс ключей типа Tkey
    /// </summary>
    public class UniversalSequence<T> : UniversalSequenceBase where T: IComparable
    {
        public UniversalSequence(PType tp_elem, Stream media) : base(tp_elem, media)
        {
        }
        // ============== Индексные дела ================
        // Предполагается, что есть некоторый ключ и последовательность отсортирована по этому ключу

        /// <summary>
        /// Функция вычисления ключа по элементу последовательности
        /// </summary>
        public Func<object, T> keyFunc = null;
        public Func<T, Diapason> getDia = null; // Задать можно снаружи (???)

        /// <summary>
        /// Поиск индекса (любого) элемента, ключ которого совпадает с образцом
        /// </summary>
        /// <param name="start"></param>
        /// <param name="number"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public long GetAnyIndexOf(long start, long number, T key)
        {
            if (number < 20)
            {
                fs.Position = 8L + start * elem_size;
                for (long i = start; i < start + number; i++)
                {
                    if (keyFunc(GetElement()).CompareTo(key) == 0) return i;
                }
            }
            long half = number / 2;
            if (number < 1)
            {// Не найден
                return -1;
            }
            long middle = start + half;
            //var middle_depth = keys[middle].CompareTo(key);
            var ob = this.GetElement(8 + middle * elem_size);
            var test_key = keyFunc(ob);
            var middle_depth = test_key.CompareTo(key);

            if (middle_depth == 0) return middle;
            if (middle_depth < 0)
            {
                return GetAnyIndexOf(middle + 1, number - half - 1, key);
            }
            else
            {
                return GetAnyIndexOf(start, half, key);
            }
        }

        public object GetAny(long start, long number, T key)
        {
            if (number < 20)
            {
                fs.Position = 8L + start * elem_size;
                for (long i = start; i < start + number; i++)
                {
                    object ob = GetElement();
                    var k = keyFunc(ob);
                    if (k.CompareTo(key) == 0) return ob;
                }
                return null;
            }
            else
            {
                long half = number / 2;
                if (number < 1)
                {// Не найден
                    return null;
                }
                long middle = start + half;
                object ob = GetElement(8L + middle * elem_size);
                var test_key = keyFunc(ob);
                var middle_depth = test_key.CompareTo(key);

                if (middle_depth == 0)
                {
                    return ob;
                }
                if (middle_depth < 0)
                {
                    return GetAny(middle + 1, number - half - 1, key);
                }
                else
                {
                    return GetAny(start, half, key);
                }
            }
        }

    }
}
