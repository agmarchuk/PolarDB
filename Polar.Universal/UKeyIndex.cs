using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Universal
{
    internal class UKeyIndex
    {
        private readonly USequence sequence;
        // Ключом является объект, порождаемый ключевой функцией. Ключи можно сравнивать!
        private Func<object, IComparable> keyFunc;
        private Func<IComparable, int> hashOfKey;
        private Dictionary<IComparable, Tuple<long, IComparable>> offkey_dic;
        private UniversalSequenceBase hkeys;
        private UniversalSequenceBase offsets;
        public UKeyIndex(Func<Stream> streamGen, USequence sequence,
            Func<object, IComparable> keyFunc, Func<IComparable, int> hashOfKey)
        {
            this.sequence = sequence;
            this.keyFunc = keyFunc;
            this.hashOfKey = hashOfKey;

            hkeys = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), streamGen());
            offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());

            offkey_dic = new Dictionary<IComparable, Tuple<long, IComparable>>();
        }

        private int[] hkeys_arr = null;

        public void Clear() { hkeys.Clear(); hkeys_arr = null; offsets.Clear(); offkey_dic.Clear(); }
        public void Flush() { hkeys.Flush(); offsets.Flush();  }
        public void Close() { hkeys.Close(); offsets.Close();  }
        public void Refresh() 
        {
            hkeys_arr = hkeys.ElementValues().Cast<int>().ToArray(); 
            offsets.Refresh(); 
        }

        public void Build() 
        {
            // сканируем опорную последовательность, формируем массивы
            List<int> hkeys_list = new List<int>();
            List<long> offsets_list = new List<long>();
            sequence.Scan((off, obj) =>
            {
                offsets_list.Add(off);
                hkeys_list.Add(hashOfKey(keyFunc(obj)));
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
            //hkeys_arr = null;
            //GC.Collect();


            offsets.Clear();
            foreach (var off in offsets_arr) { offsets.AppendElement(off); }
            offsets.Flush();
            offsets_arr = null;
            GC.Collect();
        }

        public object GetByKey(IComparable keysample)
        {
            if (offkey_dic.TryGetValue(keysample, out Tuple<long, IComparable> offkey))
            {
                return sequence.GetByOffset(offkey.Item1);
            }
            int hkey = hashOfKey(keysample);

            if (hkeys_arr != null)
            {
                int pos = Array.BinarySearch<int>(hkeys_arr, hkey);
                long offset = (long)offsets.GetByIndex(pos);
                object val = sequence.GetByOffset(offset);
                if (val == null) return null;
                var k = keyFunc(val);
                if (hashOfKey(k) != hkey) return null;
                if (k.CompareTo(keysample) == 0) return val;
            }

            //long first = GetFirstNom(hkey);
            //if (first == -1) return null;
            //for (long nom = first; nom < hkeys.Count(); nom++)
            //{
            //    long offset = (long)offsets.GetByIndex(nom);
            //    object val = sequence.GetByOffset(offset);
            //    if (val == null) break;
            //    var k = keyFunc(val);
            //    if (hashOfKey(k) != hkey) break;
            //    if (k.CompareTo(keysample) == 0) return val;
            //}
            return null;
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

        /// <summary>
        /// Определяет является ли пара (key, offset) оригиналом или нет. Если такого ключа нет в дин. индексе, то это оригинал
        /// Если есть, то надо проверить офсет
        /// </summary>
        /// <param name="key"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        private bool IsOriginal(IComparable key, long offset)
        {
            if (offkey_dic.TryGetValue(key, out Tuple<long, IComparable> offkey))
            {
                if (offkey.Item1 == offset) return true;
                return false;
            }
            return true;
        }

    }

}
