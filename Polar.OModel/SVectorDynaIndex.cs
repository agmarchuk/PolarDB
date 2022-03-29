using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.OModel
{
    public class SVectorDynaIndex : IIndex
    {
        // Ссылка на опорную последовательность
        private IBearing bearing;
        // Последовательность строк-ключей. Последовательности офсетов опорной и офсетов strings
        private string[] skeys_arr;
        private object[] elements_arr;
        // Вектораня функция
        private Func<object, IEnumerable<string>> skeyFun;
        // 
        public SVectorDynaIndex(IBearing bearing, Func<object, IEnumerable<string>> skeyFun)
        {
            this.bearing = bearing;
            // три последоваетльности с одним числом элементов и в том же порядке

            skeys_arr = new string[0];
            elements_arr = new object[0];
            this.skeyFun = skeyFun;

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
            skeys_arr = new string[0];
            elements_arr = new object[0];
        }

        public void Build()
        {
            // Не работает на всех элементах опорной последовательности, работает только на добавлении
            throw new NotImplementedException("Не работает на всех элементах опорной последовательности, работает только на добавлении");
        }

        private IEnumerable<int> Nnoms(string[] skeys, int nom)
        {
            var comp = comp_string_like; //comp_string_direct;
            string s = skeys[nom];
            // индексы влево, nom, индексы вправо

            var qu1 = Enumerable.Range(0, nom).Select(i => nom - 1 - i).TakeWhile(i => comp.Compare(skeys[i], s) == 0);
            var qu2 = Enumerable.Range(0, skeys.Length - nom - 1).Select(i => nom + i + 1).TakeWhile(i => comp.Compare(skeys[i], s) == 0);
            return qu1
                .Concat(new int[] { nom })
                .Concat(qu2);

        }
        public IEnumerable<object> LikeBySKey(string skey)
        {
            int first = Array.BinarySearch(skeys_arr, skey, comp_string_like);
            if (first < 0) return Enumerable.Empty<object>();
            return Nnoms(skeys_arr, first).Select(n => elements_arr[n]);
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public void OnAddItem(object item, long off)
        {
            string[] skey_addition = skeyFun(item).ToArray();
            if (skey_addition.Length == 0) return;
            skeys_arr = skeys_arr.Concat(skey_addition).ToArray();
            elements_arr = elements_arr.Concat(Enumerable.Repeat<object>(item, skey_addition.Length)).ToArray();
            Array.Sort(skeys_arr, elements_arr, comp_string_direct);
        }

        public void OnDeleteItem(long off)
        {
            // Пока ничего не делаем
        }

        public void Refresh()
        {
            throw new NotImplementedException();
        }
    }
}
