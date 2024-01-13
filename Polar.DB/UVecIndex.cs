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
        //private Dictionary<int, HashSet<long>> hkeyoffs_dic;

        // ============== Динамическая часть индекса =============
        class DynPairsSet
        {
            private int[] hvalues;
            private long[] offsets;
            private USequence sequ;
            Func<IComparable, int> hashOfKey;
            internal DynPairsSet(USequence sequ, Func<IComparable, int> hashOfKey)
            {
                this.sequ = sequ;
                hvalues = new int[0]; offsets = new long[0];
                this.hashOfKey = hashOfKey;
            }
            internal void Clear() { hvalues = new int[0]; offsets = new long[0]; }
            internal void OnAppendValues(IComparable[] adds, long offset)
            {
                int len = hvalues.Length;
                int nplus = adds.Length;
                if (nplus == 0) return;
                // расширим массивы
                int[] vals = new int[len + nplus];
                long[] offs = new long[len + nplus];
                for (int i = 0; i < len; i++) { vals[i] = hvalues[i]; offs[i] = offsets[i]; }
                for (int i = 0; i < nplus; i++) { vals[len + i] = hashOfKey(adds[i]); offs[len + i] = offset; }
                Array.Sort(vals, offs);
                hvalues = vals; offsets = offs;
            }
            private IEnumerable<ObjOff> GetAllByValue(IComparable valuesample)
            {
                // Определяем начальный индекс
                int ind = Array.BinarySearch(hvalues, hashOfKey(valuesample));
                if (ind >= 0)
                {

                    // Выдаем это решение
                    object rec = sequ.GetByOffset(offsets[ind]);
                    yield return new ObjOff(rec, offsets[ind]);
                    // Идем влево
                    int i = ind - 1;
                    while (i >= 0)
                    {
                        if (hvalues[i] != hashOfKey(valuesample)) break;
                        rec = sequ.GetByOffset(offsets[i]);
                        yield return new ObjOff(rec, offsets[i]);
                        i--;
                    }
                    // Идем вправо
                    i = ind + 1;
                    while (i < hvalues.Length)
                    {
                        if (hvalues[i] != hashOfKey(valuesample)) break;
                        rec = sequ.GetByOffset(offsets[i]);
                        yield return new ObjOff(rec, offsets[i]);
                        i++;
                    }
                }
            }
        }
        DynPairsSet dynindex;
        // ============ конец динамической части индекса ============



        private bool keysinmemory;
        private bool ignorecase;

        public UVecIndex(Func<Stream> streamGen, USequence sequence,
            Func<object, IEnumerable<IComparable>> keysFunc, Func<IComparable, int> hashOfKey, 
            bool ignorecase = false)
        {
            this.sequence = sequence;
            this.keysFunc = keysFunc;
            this.hashOfKey = hashOfKey;
            this.keysinmemory = true;
            this.ignorecase = ignorecase;

            hkeys = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), streamGen());
            offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());

            dynindex = new DynPairsSet(sequence, hashOfKey);
        }

        // Массив оптимизации поиска по значению хеша
        private int[]? hkeys_arr = null;

        public void Clear() { hkeys.Clear(); hkeys_arr = null; offsets.Clear(); dynindex.Clear(); }
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
            List<int>? hkeys_list = new List<int>();
            List<long>? offsets_list = new List<long>();
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
            long[]? offsets_arr = offsets_list.ToArray();
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

        public void OnAppendElement(object element, long offset)
        {
            IEnumerable<IComparable> keys = keysFunc(element);
            dynindex.OnAppendValues(keys.ToArray(), offset);
        }

        private static IEnumerable<long> LRange(long start, long numb)
        {
            for (long ii = start; ii < start + numb; ii++) yield return ii;
        }
        public IEnumerable<ObjOff> GetAllByValue(IComparable valuesample)
        {
            // Определяем начальный индекс
            if (hkeys_arr != null)
            {
                int ind = Array.BinarySearch(hkeys_arr, hashOfKey(valuesample));
                if (ind >= 0)
                {
                    // Выдаем это решение
                    long off = (long)offsets.GetByIndex(ind);
                    object rec = sequence.GetByOffset(off);
                    yield return new ObjOff(rec, off);
                    // Идем влево
                    int i = ind - 1;
                    while (i >= 0)
                    {
                        if (hkeys_arr[i] != hashOfKey(valuesample)) break;
                        off = (long)offsets.GetByIndex(i);
                        rec = sequence.GetByOffset(off);
                        yield return new ObjOff(rec, off);
                        i--;
                    }
                    // Идем вправо
                    i = ind + 1;
                    while (i < hkeys_arr.Length)
                    {
                        if (hkeys_arr[i] != hashOfKey(valuesample)) break;
                        off = (long)offsets.GetByIndex(i);
                        rec = sequence.GetByOffset(off);
                        yield return new ObjOff(rec, off);
                        i++;
                    }
                }
            }
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
