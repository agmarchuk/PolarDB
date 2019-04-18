using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class IndexKey32CompImmutable
    {
        // строится на основе последовательности пар {ключ, офсет}
        UniversalSequenceBase keyoffsets;
        private UniversalSequenceBase bearing;
        private Func<object, IEnumerable<int>> keysFun;
        private Comparer<object> comp;
        private Scale scale = null;
        public IndexKey32CompImmutable(Func<Stream> streamGen, UniversalSequenceBase bearing, 
            Func<object, IEnumerable<int>> keysFun, Comparer<object> comp)
        {
            this.bearing = bearing;
            this.keysFun = keysFun;
            this.comp = comp;
            keyoffsets = new UniversalSequenceBase(
                new PTypeRecord(
                    new NamedType("key", new PType(PTypeEnumeration.integer)),
                    new NamedType("off", new PType(PTypeEnumeration.longinteger))),
                streamGen());
            // Шкалу надо вычислять не всегда, о реальном условии еще надо подумать
            if (comp == null) scale = new Scale(streamGen());
        }
        public void Clear() { keyoffsets.Clear(); }
        public void Build()
        {
            // формируем массив пар
            List<int> keys_list = new List<int>();
            List<long> offsets_list = new List<long>();

            //int ind = 0;
            bearing.Scan((off, obj) =>
            {
                offsets_list.Add(off);
                foreach (int k in keysFun(obj))
                {
                    keys_list.Add(k);
                    //ind++;
                }
                return true;
            });
            int[] keys = keys_list.ToArray();
            keys_list = null;
            long[] offsets = offsets_list.ToArray();
            offsets_list = null;
            int ne = keys.Length;
            // Сортируем по ключу
            Array.Sort(keys, offsets);

            // Эта часть делается если компаратор объектов comp задан
            // Производится сортировка участков с одинаковыми ключами
            if (comp != null)
            {
                // массив в который будет вкладываться набор объектов с одинаковыми ключами
                List<object> objs = new List<object>();
                // проходим по массиву ключей, в группах одинаковых ключей выделяем массив объектов
                int key, start = -1; // ключ интервала и начало интервала в массивах keys и offsets 
                key = Int32.MinValue;
                // Фиксация накопленного в предыдущих переменных objs, key, start
                Action fixgroup = () =>
                {
                    int number = objs.Count;
                    if (number > 1)
                    {
                        long[] offs_small = new long[number];
                        for (int j = 0; j < number; j++)
                            offs_small[j] = offsets[start + j];
                        // Сортировка отрезка
                        Array.Sort(objs.ToArray(), offs_small, comp);
                        // вернуть отсортированные офсеты на место
                        for (int j = 0; j < number; j++)
                            offsets[start + j] = offs_small[j];
                    }
                };
                // Сканирование массивов keys, offsets
                for (int i = 0; i < ne; i++)
                {
                    int k = keys[i];
                    // смена ключа
                    if (i == 0)
                    {
                        // фиксируем предыдущий отрезок (key, start, number)
                        fixgroup();
                        // Начать новый отрезок
                        key = k;
                        start = i;
                        objs.Clear();
                    }
                    // основное действие
                    object ob = bearing.GetElement(offsets[i]);
                    objs.Add(ob);
                }
                if (objs.Count > 1) fixgroup();
            }

            // Записываем
            keyoffsets.Clear(); // очищаем
            for (int i = 0; i < keys.Length; i++)
            {
                keyoffsets.AppendElement(new object[] { keys[i], offsets[i] });
            }
            keyoffsets.Flush();

            if (scale != null) scale.Load(keys);
        }

        public void Refresh()
        {
            keyoffsets.Refresh();
        }

        public IEnumerable<object> GetAllBySample(object sample)
        {
            long start = 0;
            long number = keyoffsets.Count();
            foreach (int key in keysFun(sample))
            {
                if (scale != null && scale.GetDia != null)
                {
                    Diapason dia = scale.GetDia(key);
                    start = dia.start;
                    number = dia.numb;
                }
                foreach (var off in BinarySearchAll(start, number, key, sample))
                {
                    yield return bearing.GetElement(off);
                }
            }
        }


        const int plain = 20;
        private IEnumerable<long> BinarySearchAll(long start, long number, int key, object sample)
        {
            if (number < plain)
            {
                return keyoffsets.ElementValues(keyoffsets.ElementOffset(start), number)
                    .Where(pair => DoubleComp((object[])pair, key, sample) == 0)
                    .Select(pair => (long)((object[])pair)[1])
                    ;
            }
            long half = number / 2;
            if (half == 0)
            {
                // Получаем пару (ключ-офсет)
                object[] pair = (object[])keyoffsets.GetByIndex(start);
                int cmp = DoubleComp(pair, key, sample);
                if (cmp == 0) return Enumerable.Repeat<long>((long)pair[1], 1);
                else return Enumerable.Empty<long>(); // Не найден
            }

            long middle = start + half;
            long rest = number - half - 1;
            object[] mid_pair = (object[])keyoffsets.GetByIndex(middle);
            var middle_depth = DoubleComp(mid_pair, key, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                IEnumerable<long> flow = BinarySearchAll(start, half, key, sample).Concat(Enumerable.Repeat<long>((long)mid_pair[1], 1));
                if (rest > 0) return flow.Concat(BinarySearchAll(middle + 1, rest, key, sample));
                else return flow;
            }
            if (middle_depth < 0)
            {
                if (rest > 0) return BinarySearchAll(middle + 1, rest, key, sample);
                else return Enumerable.Empty<long>();
            }
            else
            {
                return BinarySearchAll(start, half, key, sample);
            }
        }
        private int DoubleComp(object[] pair, int key, object sample)
        {
            int k = (int)pair[0];
            int cmp = k.CompareTo(key);
            if (cmp == 0 && comp != null)
            {
                long o = (long)pair[1];
                cmp = comp.Compare(bearing.GetElement(o), sample);
            }
            return cmp;
        }

    }
}
