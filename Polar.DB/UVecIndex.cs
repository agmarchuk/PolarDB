using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Universal
{
    /// <summary>
    /// </summary>
    public class UVecIndex : IUIndex
    {
        private readonly USequence sequence;
        // Ключом является объект, порождаемый ключевой функцией. Ключи можно сравнивать!
        private Func<object, IEnumerable<IComparable>> keysFunc;
        private Func<IComparable, int> hashOfKey;
        // Статическая часть индекса
        private UniversalSequenceBase hkeys;
        private UniversalSequenceBase offsets;
        // Динамическая часть индекса
        private List<KeyValuePair<int, long>> hkeyoff_list;
        private bool keysinmemory;

        public UVecIndex(Func<Stream> streamGen, USequence sequence,
            Func<object, IEnumerable<IComparable>> keysFunc, Func<IComparable, int> hashOfKey, 
            bool keysinmemory = true)
        {
            this.sequence = sequence;
            this.keysFunc = keysFunc;
            this.hashOfKey = hashOfKey;
            this.keysinmemory = keysinmemory;

            hkeys = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), streamGen());
            offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());

            hkeyoff_list = new List<KeyValuePair<int, long>>();
        }
        public void OnAppendElement(object element, long offset)
        {
            IEnumerable<IComparable> keys = keysFunc(element);
            foreach (var key in keys)
            {
                int h = hashOfKey(key);
                hkeyoff_list.Add(new KeyValuePair<int, long>(h, offset));
            }
        }

        // Массив оптимизации поиска по значению хеша
        private int[] hkeys_arr = null;

        public void Clear() { hkeys.Clear(); hkeys_arr = null; offsets.Clear(); hkeyoff_list.Clear(); }
        public void Flush() { hkeys.Flush(); offsets.Flush(); }
        public void Close() { hkeys.Close(); offsets.Close(); }
        public void Refresh()
        {
            if (keysinmemory) hkeys_arr = hkeys.ElementValues().Cast<int>().ToArray();
            else hkeys.Refresh();
            offsets.Refresh();
        }

        public void Build()
        {
            // сканируем опорную последовательность, формируем массивы
            List<int> hkeys_list = new List<int>();
            List<long> offsets_list = new List<long>();
            sequence.Scan((off, obj) =>
            {
                var keys = keysFunc(obj);
                foreach (var key in keys)
                {
                    offsets_list.Add(off);
                    hkeys_list.Add(hashOfKey(key));
                }
                return true;
            });
            hkeys_arr = hkeys_list.ToArray();
            hkeys_list = null;
            long[] offsets_arr = offsets_list.ToArray();
            offsets_list = null;
            GC.Collect();

            Array.Sort(hkeys_arr, offsets_arr);

            hkeys.Clear();
            foreach (var hkey in hkeys_arr) { hkeys.AppendElement(hkey); }
            hkeys.Flush();
            if (!keysinmemory)
            {
                hkeys_arr = null;
                GC.Collect();
            }


            offsets.Clear();
            foreach (var off in offsets_arr) { offsets.AppendElement(off); }
            offsets.Flush();
            offsets_arr = null;
            GC.Collect();
        }

        private static IEnumerable<long> LRange(long start, long numb)
        {
            for (long ii = start; ii < start + numb; ii++) yield return ii;
        }
        public IEnumerable<object> GetByKey(IComparable keysample)
        {
            int hkey = hashOfKey(keysample);
            IEnumerable<long> offsets1 = hkeyoff_list.Where(ho => ho.Key == hkey).Select(ho => ho.Value);

            IEnumerable<long> offsets2 = Enumerable.Empty<long>();
            if (hkeys_arr != null)
            {
                int pos = Array.BinarySearch<int>(hkeys_arr, hkey);
                if (pos < 0) return null;
                // ищем самую левую позицию 
                int p1 = pos;
                while (p1 >= 0 && hkeys_arr[p1] == hkey) { p1--; }
                // движемся вправо, находим правую позицию
                int p2 = pos;
                while (p2 < hkeys_arr.Length && hkeys_arr[p2] == hkey) { p2--; }

                // Порождаем поток офсетов
                offsets2 = Enumerable.Range(p1, p2 - p1 + 1)
                    .Select(p => (long)offsets.GetByIndex(p));
            }
            else
            {
                long first = GetFirstNom(hkey);
                if (first != -1)
                {
                    offsets2 = LRange(first, first + hkeys.Count())
                        .Select(nom => (long)offsets.GetByIndex(nom));
                }
            }
            return offsets1.Concat(offsets2)
                .Select(off => (off, sequence.GetByOffset(off)))
                .Where(pair => sequence.IsOriginalAndNotEmpty(pair.Item2, pair.off))
                .Select(pair => pair.Item2);
        }

        /// <summary>
        /// Определение номера первого индекса последовательности hkeys, с которого значения РАВНЫ hkey (хешу от ключа)
        /// Если нет таких, то -1L
        /// </summary>
        /// <param name="hkey"></param>
        /// <returns></returns>
        private long GetFirstNom(int hkey)
        {
            long start = 0, end = hkeys.Count() - 1, right_equal = -1;
            // Сжимаем диапазон
            while (end - start > 1)
            {
                // Находим середину
                long middle = (start + end) / 2;
                int middle_value = (int)hkeys.GetByIndex(middle);
                if (middle_value < hkey)
                {  // Займемся правым интервалом
                    start = middle;
                }
                else if (middle_value > hkey)
                {  // Займемся левым интервалом
                    end = middle;
                }
                else
                {  // Середина дает РАВНО
                    end = middle;
                    right_equal = middle;
                }
            }
            return right_equal;
        }

        ///// <summary>
        ///// Определяет является ли пара (key, offset) оригиналом или нет. Если такого ключа нет в дин. индексе, то это оригинал
        ///// Если есть, то надо проверить офсет
        ///// </summary>
        ///// <param name="key"></param>
        ///// <param name="offset"></param>
        ///// <returns></returns>
        //public bool IsOriginal(IComparable key, long offset)
        //{
        //    if (keyoff_dic.TryGetValue(key, out long off))
        //    {
        //        if (off == offset) return true;
        //        return false;
        //    }
        //    return true; //TODO: здесь предполагается, что в основном индексе есть такое значение
        //}

    }

}
