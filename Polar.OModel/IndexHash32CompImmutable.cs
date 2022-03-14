using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.OModel
{
    public class IndexHash32CompImmutable : IIndexImmutable
    {
        private IBearing bearing;
        private Func<object, bool> applicable;
        private Func<object, int> hashFun;
        private Comparer<object> comp;
        public IndexHash32CompImmutable(Func<Stream> streamGen, IBearing bearing,
            Func<object, bool> applicable,
            Func<object, int> hashFun, Comparer<object> comp)
        {
            this.bearing = bearing;
            this.applicable = applicable;
            this.hashFun = hashFun;
            this.comp = comp;

            hkeys = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), streamGen());
            offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());
        }
        private UniversalSequenceBase hkeys;
        private UniversalSequenceBase offsets;
        //private int[] hkeys_arr;


        public void Build()
        {
            // сканируем опорную последовательность, формируем массивы
            List<int> hkeys_list = new List<int>();
            List<long> offsets_list = new List<long>();
            bearing.Scan((off, obj) =>
            {
                if (applicable != null && !applicable(obj)) return true;
                offsets_list.Add(off);
                hkeys_list.Add(hashFun(obj));
                return true;
            });
            int[] hkeys_arr = hkeys_list.ToArray();
            long[] offsets_arr = offsets_list.ToArray();
                       
            Array.Sort(hkeys_arr, offsets_arr);

            hkeys.Clear();
            foreach (var hkey in hkeys_arr) { hkeys.AppendElement(hkey); }
            hkeys.Flush();

            offsets.Clear();
            foreach (var off in offsets_arr ) { offsets.AppendElement(off); }
            offsets.Flush();

        }
        private struct Dia { public int start; public int numb; }
        Dia EqualKeys(int[] keys, int position)
        {
            int value = keys[position];
            int f = position;
            while (f - 1 >= 0 && keys[f - 1] == value) f--;
            int t = position;
            while (t + 1 < keys.Length && keys[t + 1] == value) t++;
            return new Dia() { start = f, numb = t - f + 1 };
        }
        // Ключевая процедура поиска: выработка потока (НЕ подряд стоящих) номеров ключей, совпадающих с заданым
        // Начало потока - номера, при которых в массиве ключ меньше или равен образцу ключа,
        // конец потока - номера, при которых в массиве ключ больше или равен ключу
        IEnumerable<long> GetNoms(int key)
        {
            long start = 0, end = hkeys.Count() - 1;
            // Сжимаем диапазон
            while (end - start > 4)
            {
                // Находим середину
                long middle = (start + end) / 2;
                int middle_value = (int)hkeys.GetByIndex(middle);
                if (middle_value < key)
                {  // Займемся правым интервалом
                    start = middle;
                }
                else if (middle_value > key)
                {  // Займемся левым интервалом
                    end = middle;
                }
                else // middle == key
                {  // Сформируем поток влево и вправо
                    long nom = middle - 1;
                    while (nom >= 0 && (int)hkeys.GetByIndex(nom) == key) 
                    {
                        yield return nom;
                        nom--;
                    }
                    nom = middle;
                    while (nom <hkeys.Count() && (int)hkeys.GetByIndex(nom) == key)
                    {
                        yield return nom;
                        nom++;
                    }
                    start = hkeys.Count() - 1; // Чтобы закончить
                }
            }
            long n = start;
            while (n <= end)
            {
                int val = (int)hkeys.GetByIndex(n);
                if (val > key) break;
                if (val == key) yield return n;
                n++;
            }
        }
        // Вариант процедуры, работающий с массивом hkeys_arr
        //IEnumerable<long> GetNoms0(int key)
        //{
        //    long start = 0, end = hkeys_arr.Length - 1;
        //    // Сжимаем диапазон
        //    while (end - start > 4)
        //    {
        //        // Находим середину
        //        long middle = (start + end) / 2;
        //        int middle_value = hkeys_arr[middle];
        //        if (middle_value < key)
        //        {  // Займемся правым интервалом
        //            start = middle;
        //        }
        //        else if (middle_value > key)
        //        {  // Займемся левым интервалом
        //            end = middle;
        //        }
        //        else // middle == key
        //        {  // Сформируем поток влево и вправо
        //            long nom = middle - 1;
        //            while (nom >= 0 && hkeys_arr[nom] == key)
        //            {
        //                yield return nom;
        //                nom--;
        //            }
        //            nom = middle;
        //            while (nom < hkeys_arr.Length && hkeys_arr[nom] == key)
        //            {
        //                yield return nom;
        //                nom++;
        //            }
        //            start = hkeys_arr.Length - 1; // Чтобы закончить
        //        }
        //    }
        //    long n = start;
        //    while (n <= end)
        //    {
        //        int val = hkeys_arr[n];
        //        if (val > key) break;
        //        if (val == key) yield return n;
        //        n++;
        //    }
        //}
        public object GetBySample(object sample)
        {
            return GetAllBySample(sample).FirstOrDefault();
        }
        public IEnumerable<object> GetAllBySample(object sample)
        {
            int hsample = hashFun(sample);
            return GetNoms(hsample)
                .Select(i => bearing.GetItem((long)offsets.GetByIndex(i)))
                .Where(ob => this.comp.Compare(ob, sample) == 0);
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public void Refresh()
        {
            
        }
    }
}
