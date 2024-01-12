using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Datanode
{
    class KVSequencePortion
    {
        private PType tp_element;

        //private PaCell keyvalue_seq;
        //private PaCell keys;
        //private PaCell offsets;
        private UniversalSequence<int> keyvalue_seq;
        private UniversalSequence<int> keys;
        private UniversalSequence<long> offsets;
        private Dictionary<int, long> dic1 = new Dictionary<int, long>();
        private Dictionary<int, long> dic2 = null;
        private int[] keys_arr = null;
        private Func<int, Diapason> GetDia = null;

        // Специальный индекс для внешнего ключа. Он состоит из совместно отсортированных внешнего ключа и первичного ключа опорной последовательности
        class Ndx
        {
            //public UniversalSequence<int> exkeys;
            //public UniversalSequence<int> prikeys;
            public UniversalSequence<int> exprikeys;
            public int[] key_arr;
            public Func<int, Diapason> GetDia;
            public Dictionary<int, List<int>> ekey_idlist = new Dictionary<int, List<int>>();
        }
        // Addition 
        private Ndx[] exindexes = null; // Дополнительные индексы для внешних ключей

        internal KVSequencePortion(Polar.PagedStreams.StreamStorage ss, PType tp_element, object[] stream_numbers)
        {
            //this.tp_element = tp_element;
            //PType tp_sequ = new PTypeSequence(
            //    tp_element
            //    );
            // Создадим базу данных, состоящую из последовательности и двух индексных массивов: массива ключей и массива офсетов 

            Stream stream1 = ss[(int)stream_numbers[0]];
            keyvalue_seq = new UniversalSequence<int>(tp_element, stream1);

            Stream stream2 = ss[(int)stream_numbers[1]];
            keys = new UniversalSequence<int>(new PType(PTypeEnumeration.integer), stream2);
            Stream stream3 = ss[(int)stream_numbers[2]];
            offsets = new UniversalSequence<long>(new PType(PTypeEnumeration.longinteger), stream3);

            // Addition - обработка дополнительных индексов внешних ключей 
            if (stream_numbers.Length > 3)
            {
                // По 2 на индекс
                int nadd_streams = stream_numbers.Length - 3;
                if (nadd_streams % 2 != 0) throw new Exception("Err: 22378829");
                int nadditions = nadd_streams / 2;
                exindexes = new Ndx[nadditions];
                for (int i = 0; i < nadditions; i++)
                {
                    Stream stream4 = ss[(int)stream_numbers[i * 2 + 3]];
                    //UniversalSequence<int> exkey = new UniversalSequence<int>(new PType(PTypeEnumeration.integer), stream4);
                    //Stream stream5 = ss[(int)stream_numbers[i * 2 + 4]];
                    //UniversalSequence<int> prikey = new UniversalSequence<int>(new PType(PTypeEnumeration.integer), stream5);
                    //exindexes[i] = new Ndx() { exkeys = exkey, prikeys = prikey };
                    UniversalSequence<int> exprikeys = new UniversalSequence<int>(new PTypeRecord(
                        new NamedType("exkey", new PType(PTypeEnumeration.integer)),
                        new NamedType("prikey", new PType(PTypeEnumeration.integer))), stream4);
                    exindexes[i] = new Ndx() { exprikeys = exprikeys };
                }
            }

            // Если базовая последовательность пустая - чистить все
            if (keyvalue_seq.Count() == 0) Clear();
        }
        public void Clear()
        {
            keyvalue_seq.Clear();
            //keyvalue_seq.Fill(new object[0]);
            keys.Clear(); //keys.Fill(new object[0]);
            offsets.Clear(); //offsets.Fill(new object[0]);
            if (dic1 != null) dic1 = new Dictionary<int, long>();
            if (dic2 != null) dic2 = new Dictionary<int, long>();
            if (exindexes != null)
            {
                foreach (var indx in exindexes)
                {
                    //indx.exkeys.Clear(); //indx.exkeys.Fill(new object[0]);
                    //indx.prikeys.Clear(); //indx.prikeys.Fill(new object[0]);
                    indx.exprikeys.Clear();
                }
            }
        }
        public void Activate()
        {
            if (keys_arr == null) keys_arr = keys.ElementValues().Cast<int>().ToArray();
            if (keys_arr.Length > 0) GetDia = Scale.GetDiaFunc32(keys_arr);

            if (exindexes != null)
            {
                foreach (var indx in exindexes)
                {
                    if (indx.key_arr == null) indx.key_arr = indx.exprikeys.ElementValues().Select(ob => ((object[])ob)[0]).Cast<int>().ToArray();
                    if (indx.key_arr.Length > 0) indx.GetDia = Scale.GetDiaFunc32(indx.key_arr);
                }
            }
        }
        public IEnumerable<object[]> Pairs() { return keyvalue_seq.ElementValues().Cast<object[]>(); }
        public long AppendPair(object[] pair, bool dynindex)
        {
            long offset = keyvalue_seq.AppendElement(pair);
            if (dic1 != null && dynindex) dic1.Add((int)pair[0], offset);
            return offset;
        }
        public void Flush()
        {
            keyvalue_seq.Flush();
            if (exindexes != null)
            {
                foreach (var exindx in exindexes)
                {
                    //exindx.exkeys.Flush();
                    //exindx.prikeys.Flush();
                    exindx.exprikeys.Flush();
                }
            }
        }
        public object Get(int search_key)
        {

            long offset;
            if (dic1 != null && dic1.Count > 0 && dic1.TryGetValue(search_key, out offset))
            {
                //return keyvalue_seq.Root.Element(0).SetOffset(offset).Get();
                return keyvalue_seq.GetElement(offset);
            }
            if (dic2 != null && dic2.Count > 0 && dic2.TryGetValue(search_key, out offset))
            {
                //return ((object[])keyvalue_seq.Root.Element(0).SetOffset(offset).Get())[1];
                return ((object[])keyvalue_seq.GetElement(offset))[1];
            }
            Diapason diap = GetDia(search_key);
            //int ifirst = ScaleExt.Get8FirstIndexOf((int)diap.start, (int)diap.numb, keys_arr, search_key);
            //if (ifirst == -1)
            //{
            //    return null;
            //}
            //offset = (long)offsets.Root.Element(ifirst).Get();
            //return keyvalue_seq.Root.Element(0).SetOffset(offset).Get();
            int ind = Array.BinarySearch<int>(keys_arr, search_key);
            if (ind == -1) return null;
            return keyvalue_seq.GetElement((long)offsets.GetElement((long)offsets.ElementOffset(ind)));
        }
        public void AppendExtKey(int exindnom, int extkey, int prikey, bool dynindex)
        {
            if (exindexes == null || exindexes.Length <= exindnom) throw new Exception("Err: 2983883");
            var exindx = exindexes[exindnom];
            if (dynindex)
            {
                List<int> idvalues;
                if (exindx.ekey_idlist.TryGetValue(extkey, out idvalues))
                {
                    idvalues.Add(prikey);
                }
                else exindx.ekey_idlist.Add(extkey, new List<int>(new int[] { prikey }));
            }
            else
            {
                //exindx.exkeys.AppendElement(extkey);
                //exindx.prikeys.AppendElement(prikey);
                exindx.exprikeys.AppendElement(new object[] { extkey, prikey });
            }
        }
        public int[] GetAllPrimaryByExternal(int exindnom, int exkey)
        {
            //TODO: нет концепции динамики для внешних индексов, тут должо быть решение
            if (exindexes == null || exindexes.Length <= exindnom) throw new Exception("Err: 2983883");
            var exindx = exindexes[exindnom];

            // Будут объединены результаты выборки из словаря и выборки из статического индекса
            List<int> frdic;
            int[] res;
            if (exindx.ekey_idlist.TryGetValue(exkey, out frdic)) { res = frdic.ToArray(); }
            else res = new int[0];

            if (exindx.GetDia == null) return res;
            Diapason d = exindx.GetDia(exkey);
            if (d.IsEmpty()) return res;
            int start = (int)d.start;
            int finish = start + (int)d.numb;
            for (; start < finish; start++)
            {
                if (exindx.key_arr[start] == exkey) break;
            }
            if (start == finish) return res;
            for (; finish > start; finish--) if (exindx.key_arr[finish - 1] == exkey) break;
            //var query = exindx.prikeys.Root.ElementValues(start, finish - start).Cast<int>();
            var query = Enumerable.Range(start, finish - start).Select(i => exindx.exprikeys.ElementOffset(i))
                .Select(off => ((object[])exindx.exprikeys.GetElement(off))[1]).Cast<int>();
            return res.Concat(query).ToArray();
        }


        public void CalculateDynamicIndex()
        {
            dic1 = new Dictionary<int, long>();
            //keyvalue_seq.Root.Scan((off, obj) =>
            //{
            //    int key = (int)((object[])obj)[0];
            //    dic1.Add(key, off);
            //    return true;
            //});
            int n = (int)keyvalue_seq.Count();
            for (int i = 0; i<n; i++)
            {
                long off = i == 0 ? 0L : keyvalue_seq.ElementOffset();
                object[] v = (object[])(i==0? keyvalue_seq.GetElement(0L) : keyvalue_seq.GetElement());
                int key = (int)v[0];
                dic1.Add(key, off);
            }
        }
        public void CalculateStaticIndex()
        {
            if (dic1 != null) dic1 = new Dictionary<int, long>(); // Не используем накопленное содержание
            if (dic2 != null) dic2 = new Dictionary<int, long>();
            int ind = 0;
            int nelements = (int)keyvalue_seq.Count();
            keys_arr = new int[nelements];
            long[] offs_arr = new long[nelements];
            keyvalue_seq.Scan((off, obj) =>
            {
                int key = (int)((object[])obj)[0];
                keys_arr[ind] = key;
                offs_arr[ind] = off;
                ind++;
                return true;
            });
            Array.Sort(keys_arr, offs_arr);
            keys.Clear(); //keys.Fill(new object[0]);
            List<int> keys_list = new List<int>();
            offsets.Clear(); //offsets.Fill(new object[0]);
            // Будем убирать повторы
            int prev_key = Int32.MaxValue;
            long prev_offset = Int64.MinValue;
            for (int i = 0; i < nelements; i++)
            {
                int key = keys_arr[i];
                long offset = offs_arr[i];
                if (key != prev_key)
                {  // Надо сохранить пару, но только если предыдущий ключ не фиктивный
                    if (prev_key != Int32.MaxValue)
                    {
                        keys.AppendElement(prev_key);
                        keys_list.Add(prev_key);
                        offsets.AppendElement(prev_offset);
                    }
                    prev_key = key;
                    prev_offset = offset;
                }
                else
                {
                    if (offset > prev_offset) prev_offset = offset;
                }
            }
            if (nelements > 0)
            {
                keys.AppendElement(prev_key);
                keys_list.Add(prev_key);
                offsets.AppendElement(prev_offset);
            }

            // Доделаем массив ключей
            keys_arr = keys_list.ToArray();
            keys.Flush();
            offsets.Flush();

            // Внешние индексы, если есть
            if (exindexes != null)
            {
                foreach (var exindx in exindexes)
                {
                    //var q = exindx.exkeys;
                    //// long[] offs_arr = new long[nelements];
                    //int[] ext_arr = exindx.exkeys.ElementValues().Cast<int>().ToArray();
                    //int[] pri_arr = exindx.prikeys.ElementValues().Cast<int>().ToArray();
                    //Array.Sort(ext_arr, pri_arr);
                    //exindx.exkeys.Clear(); //exindx.exkeys.Fill(new object[0]);
                    //foreach (int k in ext_arr) exindx.exkeys.AppendElement(k);
                    //exindx.prikeys.Clear(); //exindx.prikeys.Fill(new object[0]);
                    //foreach (int k in pri_arr) exindx.prikeys.AppendElement(k);
                    //exindx.exkeys.Flush();
                    //exindx.prikeys.Flush();
                    long ne = exindx.exprikeys.Count();
                    int[] ext_arr = new int[ne];
                    int[] pri_arr = new int[ne];
                    for (int i=0; i<ne; i++)
                    {
                        object[] ep = (object[])exindx.exprikeys.GetElement(exindx.exprikeys.ElementOffset(i));
                        ext_arr[i] = (int)ep[0];
                        pri_arr[i] = (int)ep[1];
                    }
                    Array.Sort(ext_arr, pri_arr);
                    exindx.exprikeys.Clear();
                    for (int i = 0; i < ne; i++)
                    {
                        exindx.exprikeys.AppendElement(new object[] { ext_arr[i], pri_arr[i] });
                    }
                    exindx.exprikeys.Flush();

                    pri_arr = null;
                    exindx.key_arr = ext_arr;
                }
            }
        }
    }
}
