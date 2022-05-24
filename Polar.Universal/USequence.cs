using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Universal
{
    public class USequence
    {
        // У универсальной последовательности нет динамической части. Все элементы доступны через методы.
        // Однако элемент может быть пустым. 
        private UniversalSequenceBase sequence;
        private Func<object, bool> isEmpty;
        private Func<object, IComparable> keyFunc;
        private UKeyIndex primaryKeyIndex;
        public IUIndex[] uindexes = null;
        
        public USequence(PType tp_el, Func<Stream> streamGen, Func<object, bool> isEmpty,
            Func<object, IComparable> keyFunc, Func<IComparable, int> hashOfKey)
        {
            sequence = new UniversalSequenceBase(tp_el, streamGen());
            this.isEmpty = isEmpty;
            this.keyFunc = keyFunc;
            primaryKeyIndex = new UKeyIndex(streamGen, this, keyFunc, hashOfKey);
        }
        public void Clear() { sequence.Clear(); primaryKeyIndex.Clear(); }
        public void Flush() { sequence.Flush(); primaryKeyIndex.Flush(); }
        public void Close() { sequence.Close(); primaryKeyIndex.Close(); }
        public void Refresh() { sequence.Refresh(); primaryKeyIndex.Refresh(); }

        public void Load(IEnumerable<object> flow)
        {
            Clear();
            foreach (var element in flow)
            {
                if (!isEmpty(element)) sequence.AppendElement(element);
            }
            primaryKeyIndex.Build();
            if (uindexes != null) 
                foreach (var ui in uindexes)
                {
                    ui.Build();
                }
            Flush();
        }
        private bool IsOriginalAndNotEmpty(object element, long off) =>
            primaryKeyIndex.IsOriginal(keyFunc(element), off) && !isEmpty(element); // сначала на оригинал, потом на пустое, может можно и иначе 


        public IEnumerable<object> ElementValues()
        {
            return sequence.ElementOffsetValuePairs()
                //// проверка на оригинал
                //.Where(pair => 
                //{
                //    long off = pair.Item1;
                //    IComparable key = keyFunc(pair.Item2);
                //    return primaryKeyIndex.IsOriginal(key, off);
                //})
                //// проверка оригиналов на пустое значение
                //.Where(pair => 
                //{
                //    return !isEmpty(pair.Item2);
                //})
                // Оставляем оригиналы и непустые
                .Where(pair => IsOriginalAndNotEmpty(pair.Item2, pair.Item1))
                .Select(pair => pair.Item2);
        }
        public void Scan(Func<long, object, bool> handler)
        {
            sequence.Scan((off, ob) => 
            {
                if (IsOriginalAndNotEmpty(ob, off)) 
                {
                    bool ok = handler(off, ob);
                    return ok;
                } 
                return true; // Реакция на не оригинал или пустой
            });
        }
        public void AppendElement(object element)
        {
            long off = sequence.AppendElement(element);
            // Корректировка индексов
            primaryKeyIndex.OnAppendElement(keyFunc(element), off);
            if (uindexes != null) foreach (var uind in uindexes) uind.OnAppendElement(keyFunc(element), off);
        }

        public object GetByKey(IComparable keysample)
        {
            return primaryKeyIndex.GetByKey(keysample);
        }

        internal object GetByOffset(long off)
        {
            return sequence.GetElement(off);
        }
        public IEnumerable<object> GetAllByIndex(int nom, object sample)
        {
            var uind = (UVectorIndex)uindexes[nom];
            IEnumerable<object> query = uind.GetAllByValue((IComparable)sample)
                .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                .Select(obof => obof.obj);
            return query;
        }
        public IEnumerable<object> GetAllByLike(int nom, object sample)
        {
            var uind = uindexes[nom];
            if (uind is UIndex)
            {
                //var query = ((UIndex)uind).GetAllByLike(sample, comp_like)
                //    .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                //    .Select(obof => obof.obj);
                //return query;
                throw new NotImplementedException("28738");
            }
            else if (uind is UVectorIndex)
            {
                var query = ((UVectorIndex)uind).GetAllByLike((string)sample)
                    .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                    .Select(obof => obof.obj);
                return query;
            }
            throw new NotImplementedException("Err: 292121");
        }

        public void Build()
        {
            this.primaryKeyIndex.Build();
            foreach (var ind in uindexes) ind.Build();
        }
    }
}
