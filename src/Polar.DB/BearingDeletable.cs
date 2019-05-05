using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Polar.DB
{
    public class BearingDeletable : IBearing
    {
        private UniversalSequenceBase sequence;
        private PType tp_elem;
        public BearingDeletable(PType tp_el, Func<Stream> streamGen)
        {
            this.tp_elem = tp_el;
            sequence = new UniversalSequenceBase(new PTypeRecord(
                new NamedType("deleted", new PType(PTypeEnumeration.boolean)),
                new NamedType("element", tp_el)), streamGen());
        }
        // Без индексов можно последовательноть очистить, загрузить поток значений, сканировать поток значений
        public void Clear()
        {
            sequence.Clear();
        }
        public void Load(IEnumerable<object> flow)
        {
            Clear();
            foreach (object element in flow)
            {
                sequence.AppendElement(new object[] { false, element });
            }
            sequence.Flush();
        }
        public void Flush() { sequence.Flush(); }
        public void Refresh() { sequence.Refresh(); }
        public IEnumerable<object> ElementValues()
        {
            return sequence.ElementValues()
                .Cast<object[]>()
                .Where(pair => !(bool)pair[0])
                .Select(pair => pair[1]);
        }
        public void Scan(Func<long, object, bool> handler)
        {
            long ll = sequence.Count();
            if (ll == 0) return;
            for (long ii = 0; ii < ll; ii++)
            {
                long off = ii == 0 ? sequence.ElementOffset(0) : sequence.ElementOffset();
                object[] full_object = (object[])sequence.GetElement();
                if ((bool)full_object[0]) continue; // если уничтожен
                bool ok = handler(off, full_object[1]);
                if (!ok) break;
            }
        }

        /// <summary>
        /// Выдает (чистый) айтем по (грязному) офсету или null 
        /// </summary>
        /// <param name="off"></param>
        /// <returns></returns>
        public object GetItem(long off)
        {
            object[] pair = (object[])sequence.GetElement(off);
            if ((bool)pair[0]) return null;
            return pair[1];
        }
        public IIndex[] Indexes { get; set; }

    }
}
