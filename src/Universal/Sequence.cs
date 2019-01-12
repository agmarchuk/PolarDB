using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Universal
{
    /// <summary>
    /// класс последовательности элементов, для которой определен ключ
    /// </summary>
    /// <typeparam name="T">параметр - тип значения ключа, если не существенен - то int</typeparam>
    public class Sequence
    {
        private readonly UniversalSequenceBase sequ;
        private List<IIndex> indexes;
        /// <summary>
        /// Конструктор порождает простую последовательность элементов указанного типа, сериализующуюся на заданном стриме 
        /// </summary>
        /// <param name="tp_elem">поляровский тип элемента</param>
        /// <param name="stream_gen">генератор стримов</param>
        /// <param name="indexes">индексы</param>
        public Sequence(PType tp_elem, Func<Stream> stream_gen, List<IIndex> indexes)
        {
            sequ = new UniversalSequenceBase(tp_elem, stream_gen());
            this.indexes = indexes;
            foreach (var index in indexes) index.BearingSequence = this; 
        }
        /// <summary>
        /// Когда последовательность определена, определяются индексы (в них указывается опорная последовательность, 
        /// сводятся в массив и напрямую задаются для работы. 
        /// </summary>
        public void Fill(IEnumerable<object> records)
        {
            sequ.Clear();
            foreach (var index in indexes) index.Clear();
            foreach (var el in records)
            {
                long off = sequ.AppendElement(el);
                //foreach (var index in indexes) index.AppendPosition(off, el);
            }
            sequ.Flush();
            foreach (var index in indexes) { index.Flush(); index.Build(); }
        }
        public void Prepare()
        {
            foreach (var index in indexes) { index.Prepare(); }
        }
        /// <summary>
        /// Предполагается, что ключ определен нулевым индексом
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public object GetElementByKey(int key)
        {
            //IndexKey32Immutable kindex = (IndexKey32Immutable)indexes[0];
            //return kindex.BinarySearchAll(key)
            //    .Select(ob =>
            //    {
            //        long off = kindex.offsetProducer(ob);
            //        return sequ.GetElement(off);
            //    })
            //    //.Where()
            //    .FirstOrDefault();
            IndexKey32Immutable kindex = (IndexKey32Immutable)indexes[0];
            var qu = kindex.BinarySearchAll(key)
                .Select(off => sequ.GetElement(off))
                .FirstOrDefault();
            return qu;
        }
        public void Scan(Func<long, object, bool> handler)
        {
            sequ.Scan(handler);
        }
        public long Count() { return sequ.Count(); }
    }
}
