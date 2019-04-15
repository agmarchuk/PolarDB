using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class IndexKey32Imm
    {
        // строится на основе последовательности пар {ключ, офсет}
        UniversalSequenceBase keyoffsets;
        private UniversalSequenceBase bearing;
        // Очень важно: функция генерирует по элементу множество ключей или полуключей
        private Func<object, IEnumerable<int>> keyFun;
        private Comparer<object> comp;
        private Scale scale = null;
        public IndexKey32Imm(Func<Stream> streamGen, UniversalSequenceBase bearing, Func<object, IEnumerable<int>> keyFun, Comparer<object> comp)
        {
            this.bearing = bearing;
            this.keyFun = keyFun;
            this.comp = comp;
            keyoffsets = new UniversalSequenceBase(
                new PTypeRecord(
                    new NamedType("key", new PType(PTypeEnumeration.integer)),
                    new NamedType("off", new PType(PTypeEnumeration.longinteger))),
                streamGen());
            // Шкалу надо вычислять не всегда, о реальном условии еще надо подумать
            if (comp == null) scale = new Scale(streamGen());
        }
        public void Build()
        {
            // формируем массив пар
            int ne = 0; //(int)bearing.Count();
            List<int> keys = new List<int>();
            List<long> offsets = new List<long>();

            int ind = 0;
            bearing.Scan((off, obj) =>
            {
                foreach (int key in keyFun(obj))
                {
                    offsets.Add(off);
                    keys.Add(key);
                    ind++;
                }
                return true;
            });
            int[] keys_arr = keys.ToArray(); keys = null;
            long[] offsets_arr = offsets.ToArray(); offsets = null;
            // Сортируем по ключу
            Array.Sort(keys_arr, offsets_arr);

            // Эта часть делается если компаратор объектов comp задан
            //if (comp != null)
            //{
            //    // массив объектов
            //    List<object> objs = new List<object>();
            //    // проходим по массиву ключей, в группах одинаковых ключей выделяем массив объектов
            //    int key, start = -1; // начало интервала и количество с одинаковым ключом  
            //    key = Int32.MinValue;
            //    Action fixgroup = () =>
            //    {
            //        int number = objs.Count;
            //        if (number > 1)
            //        {
            //            long[] offs_small = new long[number];
            //            for (int j = 0; j < number; j++)
            //                offs_small[j] = offsets[start + j];
            //            // Сортировка отрезка
            //            Array.Sort(objs.ToArray(), offs_small, comp);
            //            // вернуть отсортированные офсеты на место
            //            for (int j = 0; j < number; j++)
            //                offsets[start + j] = offs_small[j];
            //        }
            //    };
            //    for (int i = 0; i < ne; i++)
            //    {
            //        object ob = bearing.GetElement(offsets[i]);
            //        int k = keyFun(ob);
            //        // смена ключа
            //        if (i == 0 || k != key)
            //        {
            //            // фиксируем предыдущий отрезок (key, start, number)
            //            //FixGroup(offsets, objs, start);
            //            fixgroup();
            //            // Начать новый отрезок
            //            key = k;
            //            start = i;
            //            objs.Clear();
            //        }
            //        // основное действие
            //        objs.Add(ob);
            //    }
            //    if (objs.Count > 1) fixgroup();
            //}

            // Записываем
            keyoffsets.Clear(); // очищаем
            for (int i = 0; i < keys_arr.Length; i++)
            {
                keyoffsets.AppendElement(new object[] { keys_arr[i], offsets_arr[i] });
            }
            keyoffsets.Flush();

            if (scale != null) scale.Load(keys_arr);
        }

        public void Refresh()
        {
            keyoffsets.Refresh();
        }

        public IEnumerable<object> GetAllByKey(int key)
        {
            long start = 0;
            long number = keyoffsets.Count();
            if (scale != null && scale.GetDia != null)
            {
                Diapason dia = scale.GetDia(key);
                start = dia.start;
                number = dia.numb;
            }
            return GetAllByKey(start, number, key)
                .Select(off => bearing.GetElement(off));
        }


        const int plain = 25;
        private IEnumerable<long> GetAllByKey(long start, long number, int key)
        {
            if (number < plain)
            {
                return keyoffsets.ElementValues(keyoffsets.ElementOffset(start), number)
                    .Where(pair => (int)((object[])pair)[0] == key)
                    .Select(pair => (long)((object[])pair)[1]);
            }
            long half = number / 2;
            if (half == 0)
            {
                // Получаем пару (ключ-офсет)
                object[] pair = (object[])keyoffsets.GetByIndex(start);
                int cmp = ((int)pair[0]).CompareTo(key);
                if (cmp == 0) return Enumerable.Repeat<long>((long)pair[1], 1);
                else return Enumerable.Empty<long>(); // Не найден
            }

            long middle = start + half;
            long rest = number - half - 1;
            object[] mid_pair = (object[])keyoffsets.GetByIndex(middle);
            var middle_depth = ((int)mid_pair[0]).CompareTo(key);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                IEnumerable<long> flow = GetAllByKey(start, half, key).Concat(Enumerable.Repeat<long>((long)mid_pair[1], 1));
                if (rest > 0) return flow.Concat(GetAllByKey(middle + 1, rest, key));
                else return flow;
            }
            if (middle_depth < 0)
            {
                if (rest > 0) return GetAllByKey(middle + 1, rest, key);
                else return Enumerable.Empty<long>();
            }
            else
            {
                return GetAllByKey(start, half, key);
            }
        }
    }
}
