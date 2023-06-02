using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Universal
{
    internal struct HKeyObjOff
    {
        public int hkey;
        public object obj;
        public long off;
    }

    public class UIndex : IUIndex
    {
        private readonly USequence sequence;
        
        // Параметры конструктора
        private Func<object, bool> applicable;
        private Func<object, int> hashFunc;
        private Comparer<object> comp;

        // Статическая часть индекса
        private UniversalSequenceBase hkeys;
        private UniversalSequenceBase offsets;
        
        // Динамическая часть индекса: Множество троек и компаратор
        private HKeyObjOff[] dynset;
        private Comparer<HKeyObjOff> complex_comp;

        public UIndex(Func<Stream> streamGen, USequence sequence,
            Func<object, bool> applicable, Func<object, int> hashFunc, Comparer<object> comp)
        {
            this.sequence = sequence;
            this.applicable = applicable;
            this.hashFunc = hashFunc;
            this.comp = comp;

            if (hashFunc != null) hkeys = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), streamGen());
            offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());

            complex_comp = Comparer<HKeyObjOff>.Create(new Comparison<HKeyObjOff>((HKeyObjOff h1, HKeyObjOff h2) =>
            {
                int cmp;
                if (hashFunc != null)
                {
                    cmp = h1.hkey.CompareTo(h2.hkey);
                    if (cmp != 0) return cmp; 
                }
                return comp.Compare(h1.obj, h2.obj);
            }));
            dynset = new HKeyObjOff[0]; 
        }

        private int[] hkeys_arr = null;

        public void Clear() 
        { 
            if (hashFunc != null) hkeys.Clear(); 
            hkeys_arr = null; 
            offsets.Clear();
            dynset = new HKeyObjOff[0];
        }
        public void Flush() { if (hashFunc != null) hkeys.Flush(); offsets.Flush(); }
        public void Close() { if (hashFunc != null) hkeys.Close(); offsets.Close(); }
        public void Refresh()
        {
            if (hashFunc != null) hkeys_arr = hkeys.ElementValues().Cast<int>().ToArray();
            offsets.Refresh();
        }

        public void Build()
        {
            if (hashFunc == null) BuildOffsets();
            else BuildHkeyOffsets();
        }

        private Comparer<long> comp_spec_long;

        private void BuildOffsets()
        {
            comp_spec_long = Comparer<long>.Create(new Comparison<long>((off1, off2) =>
            {
                object v1 = sequence.GetByOffset(off1);
                object v2 = sequence.GetByOffset(off2);
                return comp.Compare(v1, v2);
            }));
            // сканируем опорную последовательность, формируем массивы
            List<long> offsets_list = new List<long>();
            sequence.Scan((off, obj) =>
            {
                if (applicable(obj)) offsets_list.Add(off);
                return true;
            });
            long[] offsets_arr = offsets_list.ToArray();
            offsets_list = null;
            GC.Collect();

            Array.Sort(offsets_arr, comp_spec_long);

            offsets.Clear();
            foreach (var off in offsets_arr) { offsets.AppendElement(off); }
            offsets.Flush();
            offsets_arr = null;
            GC.Collect();
        }
        // hashFunc != null
        private void BuildHkeyOffsets()
        {
            // сканируем опорную последовательность, формируем массивы
            List<int> hkeys_list = new List<int>();
            List<long> offsets_list = new List<long>();
            sequence.Scan((off, obj) =>
            {
                offsets_list.Add(off);
                hkeys_list.Add(hashFunc(obj));
                return true;
            });
            if (hashFunc != null) hkeys_arr = hkeys_list.ToArray();
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
        internal IEnumerable<ObjOff> GetAllBySample(object sample)
        {
            if (dynset.Count() > 0)
            {
                HKeyObjOff complex_sample = new HKeyObjOff() { obj = sample };
                if (hashFunc != null) complex_sample.hkey = hashFunc(sample);
                var query = dynset.Where(hoo => complex_comp.Compare(hoo, complex_sample) == 0)
                    .Select(hoo => new ObjOff(hoo.obj, hoo.off));
                foreach (var oo in query)
                {
                    yield return oo;
                }
            }
            long first = GetFirstNomOffsets(sample, comp);
            for (long ii = first; ii < offsets.Count(); ii++)
            {
                long off = (long)offsets.GetByIndex(ii);
                object value = sequence.GetByOffset(off);
                if (comp.Compare(value, sample) == 0) yield return new ObjOff(value, off);
                else break;
            }
        }
        internal IEnumerable<ObjOff> GetAllByLike(object sample, Comparer<object> comp_like)
        {
            if (dynset.Count() > 0)
            {
                var query = dynset.Select(hoo => new ObjOff(hoo.obj, hoo.off));
                foreach (var oo in query)
                {
                    if( comp_like.Compare(oo.obj, sample) == 0 ) yield return oo;
                }
            }
            long first = GetFirstNomOffsets(sample, comp_like);
            for (long ii = first; ii < offsets.Count(); ii++)
            {
                long off = (long)offsets.GetByIndex(ii);
                object value = sequence.GetByOffset(off);
                if (comp_like.Compare(value, sample) == 0) yield return new ObjOff(value, off);
                else break;
            }
        }
        public void OnAppendElement(object element, long offset)
        {
            throw new NotImplementedException("21298");
        }


        private long GetFirstNomOffsets(object sample, Comparer<object> comparer)
        {
            long start = 0, end = offsets.Count() - 1, right_equal = -1;
            // Сжимаем диапазон
            int cmp = 0;
            object middle_value = null;
            while (end - start > 1)
            {
                // Находим середину
                long middle = (start + end) / 2;
                middle_value = sequence.GetByOffset((long)offsets.GetByIndex(middle));
                cmp = comparer.Compare(middle_value, sample);
                if (cmp < 0)
                {  // Займемся правым интервалом
                    start = middle;
                }
                else if (cmp > 0)
                {  // Займемся левым интервалом
                    end = middle;
                }
                else
                {  // Середина дает РАВНО
                    end = middle;
                    right_equal = middle;
                }
            }
            // Если нуля не было, проверить другой конец отрезка (start, middle) или (middle, end)
            if (right_equal == -1)
            {
                long another = cmp < 0 ? end : start;
                middle_value = sequence.GetByOffset((long)offsets.GetByIndex(another));
                cmp = comparer.Compare(middle_value, sample);
                if (cmp == 0) return another;
            }
            return right_equal;
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

    }

}
