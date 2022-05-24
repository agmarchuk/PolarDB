using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Universal
{
    public class UVectorIndex : IUIndex
    {
        private readonly USequence sequence;
        // Ключом является объект, порождаемый ключевой функцией. Ключи можно сравнивать!
        Func<object, IEnumerable<IComparable>> valuesFunc;
        // Статическая часть индекса
        private UniversalSequenceBase values;
        private UniversalSequenceBase element_offsets;
        // Динамическая часть индекса
        private Dictionary<IComparable, long[]> valueoffs_dic;

        /// <summary>
        /// На каждом элементе опорной последовательности sequence вычисляется вектораная функция valuesFunc.
        /// Полученные величины складируется в последовательность values. Последовательности values,
        /// и element_offsets согласованы между собой (i-ый элемент относится к одному). В рабочем состоянии
        /// предполагается отсортированность пары по values
        /// </summary>
        /// <param name="streamGen"></param>
        /// <param name="sequence"></param>
        /// <param name="tp_value">Тип value</param>
        /// <param name="valuesFunc"></param>
        public UVectorIndex(Func<Stream> streamGen, USequence sequence, PType tp_value,
            Func<object, IEnumerable<IComparable>> valuesFunc)
        {
            this.sequence = sequence;
            this.valuesFunc = valuesFunc;

            values = new UniversalSequenceBase(tp_value, streamGen());
            element_offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());

            valueoffs_dic = new Dictionary<IComparable, long[]>();

            comp_string = Comparer<IComparable>.Create(new Comparison<IComparable>((IComparable v1, IComparable v2) =>
            {
                string a = (string)v1;
                string b = (string)v2;
                if (string.IsNullOrEmpty(b)) return 0;
                return string.Compare(a, b, StringComparison.OrdinalIgnoreCase);
            }));

            comp_string_like = Comparer<IComparable>.Create(new Comparison<IComparable>((IComparable v1, IComparable v2) =>
            {
                string a = (string)v1;
                string b = (string)v2;
                if (string.IsNullOrEmpty(b)) return 0;
                int len = b.Length;
                return string.Compare(
                    a, 0,
                    b, 0, len, StringComparison.OrdinalIgnoreCase);
            }));
        }
        
        private Comparer<IComparable> comp_string;
        private Comparer<IComparable> comp_string_like;

        public void OnAppendElement(IComparable key, long offset)
        {
            if (valueoffs_dic.TryGetValue(key, out long[] offsets))
            {
                offsets = offsets.Append(offset).ToArray();
            }
            valueoffs_dic.Add(key, new long[] { offset });
        }

        // Массив оптимизации поиска по значению value
        private IComparable[] values_arr = null;

        public void Clear() { values.Clear(); element_offsets.Clear(); values_arr = new IComparable[0]; valueoffs_dic = new Dictionary<IComparable, long[]>(); }
        public void Flush() { values.Flush(); element_offsets.Flush(); }
        public void Close() { values.Close(); element_offsets.Close(); }
        public void Refresh()
        {
            values_arr = values.ElementValues().Cast<IComparable>().ToArray();
            element_offsets.Refresh();
        }

        public void Build()
        {
            // сканируем опорную последовательность, формируем массивы
            List<IComparable> values_list = new List<IComparable>();
            List<long> offsets_list = new List<long>();
            sequence.Scan((off, obj) =>
            {
                var vals = valuesFunc(obj);
                foreach (var v in vals)
                {
                    offsets_list.Add(off);
                    values_list.Add(v);
                }
                return true;
            });
            values_arr = values_list.ToArray();
            values_list = null;
            long[] offsets_arr = offsets_list.ToArray();
            offsets_list = null;
            GC.Collect();

            Array.Sort(values_arr, offsets_arr, comp_string);

            values.Clear();
            foreach (var v in values_arr) { values.AppendElement(v); }
            values.Flush();

            element_offsets.Clear();
            foreach (var off in offsets_arr) { element_offsets.AppendElement(off); }
            element_offsets.Flush();
            offsets_arr = null;
            GC.Collect();
        }

        internal IEnumerable<ObjOff> GetAllByValue(IComparable valuesample)
        {
            if (valueoffs_dic.TryGetValue(valuesample, out long[] offs))
            {
                foreach (var oo in offs.Select(o => new ObjOff(sequence.GetByOffset(o), o)))
                {
                    yield return oo;
                }
            }

            int pos = Array.BinarySearch<IComparable>(values_arr, valuesample, comp_string);
            // ищем самую левую позицию 
            int p = pos;
            while (p >= 0 && values_arr[p].CompareTo(valuesample) == 0) { pos = p; p--; }
            // движемся вправо
            while (pos < values_arr.Length && values_arr[pos].CompareTo(valuesample) == 0)
            {
                long offset = (long)element_offsets.GetByIndex(pos);
                object ob = sequence.GetByOffset(offset);
                yield return new ObjOff(ob, offset);
                pos++;
            }
        }

        internal IEnumerable<ObjOff> GetAllByLike(string valuesample)
        {
            string[] strings_arr = values_arr.Cast<string>().ToArray();
            //if (valueoffs_dic.TryGetValue(valuesample, out long[] offs))
            //{
            //    foreach (var oo in offs.Select(o => new ObjOff(sequence.GetByOffset(o), o)))
            //    {
            //        yield return oo;
            //    }
            //} StringComparison.OrdinalIgnoreCase
            
            int pos = Array.BinarySearch<string>(strings_arr, valuesample, comp_string_like);
            if (pos < 0) goto Fin;
            // ищем самую левую позицию 
            int p = pos;
            while (p >= 0 && comp_string_like.Compare(values_arr[p], valuesample) == 0) { pos = p; p--; }
            // движемся вправо
            while (pos < values_arr.Length && comp_string_like.Compare(values_arr[pos], valuesample) == 0)
            {
                long offset = (long)element_offsets.GetByIndex(pos);
                object ob = sequence.GetByOffset(offset);
                yield return new ObjOff(ob, offset);
                pos++;
            }
            Fin: { }
        }

    }

}
