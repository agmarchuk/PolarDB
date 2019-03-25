using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class IndexKey32CompImm
    {
        // строится на основе последовательности пар {ключ, офсет}
        UniversalSequenceBase keyoffsets;
        //private Func<Stream> streamGen;
        private UniversalSequenceBase bearing;
        private Func<object, int> keyFun;
        private Comparer<object> comp;
        private Scale scale;
        public IndexKey32CompImm(Func<Stream> streamGen, UniversalSequenceBase bearing, Func<object, int> keyFun, Comparer<object> comp)
        {
            this.bearing = bearing;
            this.keyFun = keyFun;
            this.comp = comp;
            keyoffsets = new UniversalSequenceBase(
                new PTypeRecord(
                    new NamedType("key", new PType(PTypeEnumeration.integer)),
                    new NamedType("off", new PType(PTypeEnumeration.longinteger))),
                streamGen());
            scale = new Scale(streamGen());
        }
        struct KeyOffPair { public int key; public long off; }
        KeyOffPair[] keyoff_arr;
        Comparer<KeyOffPair> comparer;
        private Func<int, Diapason> scaleFunc = null;
        public void Build()
        {
            // формируем два массива
            int ne = (int)bearing.Count();
            keyoff_arr = new KeyOffPair[ne];
            int ind = 0;
            bearing.Scan((off, obj) =>
            {
                keyoff_arr[ind].off = off;
                keyoff_arr[ind].key = keyFun(obj);
                ind++;
                return true;
            });
            // Вычисляем компаратор
            bool detail_sorting = false; // пока будем сортировать "грубо"
            comparer = Comparer<KeyOffPair>.Create(new Comparison<KeyOffPair>((KeyOffPair a, KeyOffPair b) =>
            {
                int cmp = a.key.CompareTo(b.key);
                if (cmp != 0 || !detail_sorting) return cmp;
                object obja = keyoffsets.GetElement(a.off);
                object objb = keyoffsets.GetElement(b.off);
                return comp.Compare(obja, objb);
            }));

            // Сортируем
            Array.Sort(keyoff_arr, comparer);

            // Записываем
            keyoffsets.Clear(); // очищаем
            for (int i=0; i<keyoff_arr.Length; i++)
            {
                keyoffsets.AppendElement(new object[] { keyoff_arr[i].key, keyoff_arr[i].off });
            }
            keyoffsets.Flush();

            scaleFunc = Scale.GetDiaFunc32(keyoff_arr.Select(ko => ko.key).ToArray());
            scale.Load(keyoff_arr.Select(ko => ko.key).ToArray());
        }
        public void Refresh()
        {
            keyoffsets.Refresh();
        }

        public IEnumerable<object> GetAllByKey(int key)
        {
            var comparer_simple = Comparer<KeyOffPair>.Create(new Comparison<KeyOffPair>((KeyOffPair a, KeyOffPair b) =>
            {
                int cmp = a.key.CompareTo(b.key);
                return cmp;
            }));
            KeyOffPair sample = new KeyOffPair() { key = key };
            int pos = Array.BinarySearch<KeyOffPair>(keyoff_arr, sample, comparer_simple);
            if (pos == -1) return Enumerable.Empty<object>();
            // Отступаем назад так, чтобы на элементах был тот же ключ и чтобы "срабатывал" компаратор comp
            while (pos - 1 >= 0 && keyoff_arr[pos - 1].key == key) pos = pos - 1;
            var res = keyoff_arr.Skip(pos)
                .TakeWhile(pair => pair.key == key)
                .Select(pair =>
                {
                    long o = pair.off;
                    return bearing.GetElement(o);
                });
            return res;
        }


        public IEnumerable<object> GetBySubj(int subj)
        {
            bool inmemory = false;
            if (inmemory) return GetAllByKey(subj);
            long start = 0;
            long number = keyoffsets.Count();
            //if (scaleFunc != null)
            //{
            //    Diapason dia = scaleFunc(subj);
            //    start = dia.start;
            //    number = dia.numb;
            //}
            if (scale.GetDia != null)
            {
                Diapason dia = scale.GetDia(subj);
                start = dia.start;
                number = dia.numb;
            }
            return BinarySearchAll(start, number, subj, new object[] { subj, -1, null })
                .Select(off => bearing.GetElement(off));
        }


        const int plain = 20;
        public IEnumerable<long> BinarySearchAll(long start, long number, int key, object sample)
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
