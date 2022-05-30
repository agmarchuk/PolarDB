using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Universal
{
    public class SVectorIndex : IUIndex
    {
        private readonly USequence sequence;
        // Функция, порождающая набор слов из объекта последовательности
        Func<object, IEnumerable<string>> valuesFunc;
        // Статическая часть индекса состоит из согласованных последовательностей строк и офсетов элементов опорной
        // последовательности из которых получились строки 
        private UniversalSequenceBase values;
        private UniversalSequenceBase element_offsets;
        // Динамическая часть индекса: множестов пар строка-множество офсетов, оптимизирована по доступу от строки
        private Dictionary<string, long[]> valueoffs_dic;

        /// <summary>
        /// На каждом элементе опорной последовательности sequence вычисляется вектораная функция valuesFunc.
        /// Полученные величины складируется в последовательности values и element_offsets. Последовательности values,
        /// и element_offsets согласованы между собой (i-ый элемент относится к одному). 
        /// </summary>
        /// <param name="streamGen"></param>
        /// <param name="sequence"></param>
        /// <param name="valuesFunc"></param>
        public SVectorIndex(Func<Stream> streamGen, USequence sequence, Func<object, IEnumerable<string>> valuesFunc)
        {
            this.sequence = sequence;
            this.valuesFunc = valuesFunc;

            values = new UniversalSequenceBase(new PType(PTypeEnumeration.sstring), streamGen());
            element_offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());

            valueoffs_dic = new Dictionary<string, long[]>();

            // Компараторы для строк
            comp_string = Comparer<IComparable>.Create(new Comparison<IComparable>((IComparable v1, IComparable v2) =>
            {
                string a = (string)v1;
                string b = (string)v2;
                //if (string.IsNullOrEmpty(b)) return 0;
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

        // Массив оптимизации поиска по значению value
        private string[] values_arr = null;

        public void Clear() { values.Clear(); element_offsets.Clear(); values_arr = new string[0]; valueoffs_dic = new Dictionary<string, long[]>(); }
        public void Flush() { values.Flush(); element_offsets.Flush(); }
        public void Close() { values.Close(); element_offsets.Close(); }
        public void Refresh()
        {
            values_arr = values.ElementValues().Cast<string>().ToArray();
            element_offsets.Refresh();
        }

        public void Build()
        {
            // сканируем опорную последовательность, формируем массивы
            List<string> values_list = new List<string>();
            List<long> offsets_list = new List<long>();
            sequence.Scan((off, obj) =>
            {
                var vals = valuesFunc(obj);
                foreach (var v in vals)
                {
                    if (string.IsNullOrEmpty(v)) continue;
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


        public void OnAppendElement(object element, long offset)
        {
            var values = valuesFunc(element);
            foreach (var value in values)
            {
                string str = (string)value;
                if (valueoffs_dic.TryGetValue(str, out long[] offsets))
                {
                    offsets.Append(offset).ToArray();
                }
                else valueoffs_dic.Add(str, new long[] { offset });
            }
        }

        internal IEnumerable<ObjOff> GetAllByValue(IComparable valuesample)
        {
            string svalue = (string)valuesample;
            if (valueoffs_dic.TryGetValue(svalue, out long[] offs))
            {
                foreach (var oo in offs.Select(o => new ObjOff(sequence.GetByOffset(o), o)))
                {
                    yield return oo;
                }
            }
            int pos = Array.BinarySearch<IComparable>(values_arr, svalue, comp_string);
            if (pos >= 0)
            {
                // ищем самую левую позицию 
                int p = pos;
                while (p >= 0 && comp_string.Compare(values_arr[p], svalue) == 0) { pos = p; p--; }
                // движемся вправо
                while (pos < values_arr.Length && comp_string.Compare(values_arr[pos], svalue) == 0)
                {
                    long offset = (long)element_offsets.GetByIndex(pos);
                    object ob = sequence.GetByOffset(offset);
                    yield return new ObjOff(ob, offset);
                    pos++;
                }
            }
        }

        internal IEnumerable<ObjOff> GetAllByLike(string svalue)
        {
            foreach (KeyValuePair<string, long[]> pair in valueoffs_dic)
            {
                if (comp_string_like.Compare(pair.Key, svalue) == 0)
                {
                    foreach (var oo in pair.Value.Select(o => new ObjOff(sequence.GetByOffset(o), o)))
                    {
                        yield return oo;
                    }
                }
            }
            int pos = Array.BinarySearch<string>(values_arr, svalue, comp_string_like);
            if (pos >= 0)
            {
                // ищем самую левую позицию 
                int p = pos;
                while (p >= 0 && comp_string_like.Compare(values_arr[p], svalue) == 0) { pos = p; p--; }
                // движемся вправо
                while (pos < values_arr.Length && comp_string_like.Compare(values_arr[pos], svalue) == 0)
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
