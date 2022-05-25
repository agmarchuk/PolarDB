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
        }
        
        public void OnAppendElement(object element, long offset)
        {
            var values = valuesFunc(element);
            foreach (var value in values)
            {
                IComparable key = value;
                if (valueoffs_dic.TryGetValue(key, out long[] offsets))
                {
                    offsets.Append(offset).ToArray();
                }
                else valueoffs_dic.Add(key, new long[] { offset });
            }
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

            Array.Sort(values_arr, offsets_arr);

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
            int pos = Array.BinarySearch<IComparable>(values_arr, valuesample);
            if (pos >= 0) 
            {
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
        }
    }

}
