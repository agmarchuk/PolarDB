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

        // Компараторы для строк
        public static Comparer<IComparable> comp_string = Comparer<IComparable>.Create(new Comparison<IComparable>((IComparable v1, IComparable v2) =>
        {
            string a = (string)v1;
            string b = (string)v2;
            //if (string.IsNullOrEmpty(b)) return 0;
            return string.Compare(a, b, StringComparison.OrdinalIgnoreCase);
        }));
        public static Comparer<IComparable> comp_string_like = Comparer<IComparable>.Create(new Comparison<IComparable>((IComparable v1, IComparable v2) =>
        {
            string a = (string)v1;
            string b = (string)v2;
            if (string.IsNullOrEmpty(b)) return 0;
            int len = b.Length;
            return string.Compare(
                a, 0,
                b, 0, len, StringComparison.OrdinalIgnoreCase);
        }));

        // Динамическая часть индекса
        class DynPairsSet
        {
            private string[] svalues;
            private long[] offsets;
            private USequence sequ;
            internal DynPairsSet(USequence sequ) 
            {
                this.sequ = sequ;
                svalues = new string[0]; offsets = new long[0];
            }
            internal void Clear() { svalues = new string[0]; offsets = new long[0]; }
            internal void OnAppendValues(string[] adds, long offset)
            {
                int len = svalues.Length;
                int nplus = adds.Length;
                if (nplus == 0) return;
                // расширим массивы
                string[] vals = new string[len + nplus];
                long[] offs = new long[len + nplus];
                for (int i=0; i<len; i++) { vals[i] = svalues[i]; offs[i] = offsets[i]; }
                for (int i=0; i<nplus; i++ ) { vals[len + i] = adds[i]; offs[len + i] = offset; }
                Array.Sort(vals, offs, comp_string);
                svalues = vals; offsets = offs;
            }
            private IEnumerable<ObjOff> GetAllByComp(IComparable valuesample, Comparer<IComparable> comp_s)
            {
                // Определяем начальный индекс
                int ind = Array.BinarySearch(svalues, valuesample, comp_s);
                if (ind >= 0)
                {
                    
                    // Выдаем это решение
                    object rec = sequ.GetByOffset(offsets[ind]);
                    yield return new ObjOff(rec, offsets[ind]);
                    // Идем влево
                    int i = ind - 1;
                    while (i >= 0)
                    {
                        if (comp_s.Compare(svalues[i], valuesample) != 0) break;
                        rec = sequ.GetByOffset(offsets[i]);
                        yield return new ObjOff(rec, offsets[i]);
                        i--;
                    }
                    // Идем вправо
                    i = ind + 1;
                    while (i < svalues.Length)
                    {
                        if (comp_s.Compare(svalues[i], valuesample) != 0) break;
                        rec = sequ.GetByOffset(offsets[i]);
                        yield return new ObjOff(rec, offsets[i]);
                        i++;
                    }
                }
            }
            internal IEnumerable<ObjOff> GetAllByValue(IComparable valuesample) => GetAllByComp(valuesample, comp_string);
            internal IEnumerable<ObjOff> GetAllByLike(IComparable valuesample) => GetAllByComp(valuesample, comp_string_like);
        }
        DynPairsSet dynindex; 

        /// <summary>
        /// На каждом элементе опорной последовательности sequence вычисляется векторная функция valuesFunc.
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

            dynindex = new DynPairsSet(sequence);
        }

        // Массив оптимизации поиска по значению value
        private string[] values_arr = null;

        public void Clear() { values.Clear(); element_offsets.Clear(); values_arr = new string[0]; dynindex.Clear(); }
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
            dynindex.OnAppendValues(values.ToArray(), offset);
        }

        private IEnumerable<ObjOff> GetAllByComp(IComparable valuesample, Comparer<IComparable> comp_s)
        {
            // Определяем начальный индекс
            int ind = Array.BinarySearch(values_arr, valuesample, comp_s);
            if (ind >= 0)
            {

                // Выдаем это решение
                long off = (long)element_offsets.GetByIndex(ind);
                object rec = sequence.GetByOffset(off);
                yield return new ObjOff(rec, off);
                // Идем влево
                int i = ind - 1;
                while (i >= 0)
                {
                    if (comp_s.Compare(values_arr[i], valuesample) != 0) break;
                    off = (long)element_offsets.GetByIndex(i);
                    rec = sequence.GetByOffset(off);
                    yield return new ObjOff(rec, off);
                    i--;
                }
                // Идем вправо
                i = ind + 1;
                while (i < values_arr.Length)
                {
                    if (comp_s.Compare(values_arr[i], valuesample) != 0) break;
                    off = (long)element_offsets.GetByIndex(i);
                    rec = sequence.GetByOffset(off);
                    yield return new ObjOff(rec, off);
                    i++;
                }
            }
        }
        internal IEnumerable<ObjOff> GetAllByValue(IComparable valuesample)
        {
            string svalue = (string)valuesample;
            var query = dynindex.GetAllByValue(valuesample);
            foreach (var v in query)
            {
                yield return v; 
            }
            var qu = GetAllByComp(valuesample, comp_string);
            foreach (var v in qu)
            {
                yield return v;
            }
        }

        internal IEnumerable<ObjOff> GetAllByLike(string svalue)
        {
            var query = dynindex.GetAllByLike(svalue);
            foreach (var v in query)
            {
                yield return v;
            }
            var qu = GetAllByComp(svalue, comp_string_like).ToArray();
            foreach (var v in qu)
            {
                yield return v;
            }
        }

    }
}
