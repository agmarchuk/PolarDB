using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.OModel
{
    public class SVectorIndex : IIndex
    {
        // Ссылка на опорную последовательность
        private IBearing bearing;
        // Последовательность строк-ключей. Последовательности офсетов опорной и офсетов strings
        private UniversalSequenceBase strings;
        //private UniversalSequenceBase bearing_strings_offsets;
        private UniversalSequenceBase boffsets;
        private UniversalSequenceBase soffsets;
        private Func<object, IEnumerable<string>> skeyFun;
        // 
        public SVectorIndex(Func<Stream> streamGen, IBearing bearing, Func<object, IEnumerable<string>> skeyFun)
        {
            this.bearing = bearing;
            // три последоваетльности с одним числом элементов и в том же порядке
            this.strings = new UniversalSequenceBase(new PType(PTypeEnumeration.sstring), streamGen());
            this.boffsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());
            this.soffsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());

            this.skeyFun = skeyFun;

            if (bearing.Indexes == null) bearing.Indexes = new IIndex[] { this };
            else bearing.Indexes = bearing.Indexes.Append(this).ToArray();

            comp_string_direct = Comparer<string>.Create(new Comparison<string>((string a, string b) =>
            {
                string val1 = a;
                string val2 = b;
                return string.Compare(val1, val2, StringComparison.OrdinalIgnoreCase);
            }));

            comp_string_like = Comparer<string>.Create(new Comparison<string>((string a, string b) =>
            {
                string val1 = a;
                string val2 = b;
                if (string.IsNullOrEmpty(val2)) return 0;
                int len = val2.Length;
                return string.Compare(
                    val1, 0,
                    val2, 0, len, StringComparison.OrdinalIgnoreCase);
            }));
        }

        private Comparer<string> comp_string_direct, comp_string_like;

        public void Clear()
        {
            strings.Clear();
            boffsets.Clear();
            soffsets.Clear();
        }

        public void Build()
        {
            this.strings.Clear();
            this.boffsets.Clear();
            this.soffsets.Clear();
            // сканируем опорную последовательность, формируем массивы
            List<string> skeys_list = new List<string>();
            List<Tuple<long, long>> offsets_list = new List<Tuple<long, long>>();
            bearing.Scan((off, obj) =>
            {
                foreach (string skey in skeyFun(obj)) 
                {
                    skeys_list.Add(skey);
                    long soff = this.strings.AppendElement(skey);
                    offsets_list.Add(new Tuple<long, long>(off, soff));
                }
                return true;
            });
            this.strings.Flush();
            //string[] skeys_arr = skeys_list.ToArray();
            //Tuple<long, long>[] offsets_arr = offsets_list.ToArray();
            skeys_arr = skeys_list.ToArray();
            offsets_arr = offsets_list.ToArray();

            Array.Sort(skeys_arr, offsets_arr, comp_string_direct);

            boffsets.Clear();
            soffsets.Clear();
            foreach (var off in offsets_arr) 
            { 
                boffsets.AppendElement(off.Item1);
                soffsets.AppendElement(off.Item2);
            }
            boffsets.Flush();
            soffsets.Flush();
            offsets_arr = null;
            // можно не уничтожать, тогда допустима либо оптимизация, либо отладка
            skeys_arr = null;
        }
        private string[] skeys_arr;
        private Tuple<long, long>[] offsets_arr;

        private IEnumerable<int> Nnoms(string[] skeys, int nom)
        {
            var comp = comp_string_like; //comp_string_direct;
            string s = skeys[nom];
            // индексы влево, nom, индексы вправо
            var qu1 = Enumerable.Range(0, nom).Select(i => nom - 1 - i).TakeWhile(i => comp.Compare(skeys[i], s) == 0);
            var qu2 = Enumerable.Range(0, skeys.Length - nom - 1).Select(i => nom + i).TakeWhile(i => comp.Compare(skeys[i], s) == 0);
            return qu1
                .Concat(qu2);

        }
        public IEnumerable<object> LikeBySKey(string skey)
        {
            if (skeys_arr == null)
            {
                return GetNoms(skey, comp_string_like).Select(n => bearing.GetItem((long)boffsets.GetByIndex(n)));
            }
            else
            {
                int first = Array.BinarySearch(skeys_arr, skey, comp_string_like);
                if (first < 0) return Enumerable.Empty<object>();
                return Nnoms(skeys_arr, first).Select(n => bearing.GetItem((long)boffsets.GetByIndex(n)));
            }
        }

        public IEnumerable<object> GetBySKey(string skey, Comparer<string> comp)
        {
            return GetNoms(skey, comp).Select(n => bearing.GetItem((long)boffsets.GetByIndex(n)));
        }
        private IEnumerable<long> GetNoms(string skey, IComparer<string> comp)
        {
            Func<string, string, int> srav = (s1, s2) => comp == null ? s1.CompareTo(s2) : comp.Compare(s1, s2); // string.Compare(s1, 0, s2, 0, s2.Length);
            long start = 0, end = soffsets.Count() - 1;
            // Сжимаем диапазон
            while (end - start > 4)
            {
                // Находим середину
                long middle = (start + end) / 2;
                string middle_value = (string)strings.GetElement((long)soffsets.GetByIndex(middle));
                //int cmp =  middle_value.CompareTo(skey);
                int cmp = srav(middle_value, skey);
                if (cmp < 0)
                {  // Займемся правым интервалом
                    start = middle;
                }
                else if (cmp > 0)
                {  // Займемся левым интервалом
                    end = middle;
                }
                else // middle == key
                {  // Сформируем поток влево и вправо
                    long nom = middle - 1;
                    while (nom >= 0 && srav((string)strings.GetElement((long)soffsets.GetByIndex(nom)), skey) == 0)
                    {
                        yield return nom;
                        nom--;
                    }
                    nom = middle;
                    while (nom < soffsets.Count() && srav((string)strings.GetElement((long)soffsets.GetByIndex(nom)), skey) == 0)
                    {
                        yield return nom;
                        nom++;
                    }
                    start = soffsets.Count() - 1; // Чтобы закончить
                }
            }
            long n = start;
            while (n <= end)
            {
                string val = (string)strings.GetElement((long)soffsets.GetByIndex(n));
                int cm = srav(val, skey);
                if (cm > 0) break;
                if (cm == 0) yield return n;
                n++;
            }

        }
        //public IEnumerable<object> GetBySKey0(string skey)
        //{
        //    foreach (var offoff in bearing_strings_offsets.ElementValues())
        //    {
        //        string s = (string)this.strings.GetElement((long)((object[])offoff)[1]);
        //        if (s.StartsWith(skey))
        //        {
        //            yield return bearing.GetItem((long)((object[])offoff)[0]);
        //        }
        //    }
        //}


        public void Flush()
        {
            throw new NotImplementedException();
        }

        public void OnAddItem(object item, long off)
        {
            throw new NotImplementedException();
        }

        public void OnDeleteItem(long off)
        {
            throw new NotImplementedException();
        }

        public void Refresh()
        {
            throw new NotImplementedException();
        }
    }
}
