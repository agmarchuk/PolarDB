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
        private UKeyIndex primaryKeyIndex;
        public USequence(PType tp_el, Func<Stream> streamGen, Func<object, bool> isEmpty,
            Func<object, IComparable> keyFunc, Func<IComparable, int> hashOfKey)
        {
            sequence = new UniversalSequenceBase(tp_el, streamGen());
            this.isEmpty = isEmpty;
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
            Flush();
        }
        public void Scan(Func<long, object, bool> handler)
        {
            sequence.Scan(handler);
        }
        public void AppendElement(object element)
        {
            if (!isEmpty(element))
            {
                long off = sequence.AppendElement(element);
                // Кооректировка индексов
            }
                   
        }

        public object GetByKey(IComparable keysample)
        {
            return primaryKeyIndex.GetByKey(keysample);
        }

        internal object GetByOffset(long off)
        {
            if (off >= 0)
            {
                var v = sequence.GetElement(off);
                if (!isEmpty(v))  return v;
            }
            return null;
        }
    }
}
