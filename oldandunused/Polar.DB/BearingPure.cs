using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Polar.DB
{
    public class BearingPure : IBearing
    {
        private UniversalSequenceBase sequence;
        private PType tp_elem;
        public BearingPure(PType tp_el, Func<Stream> streamGen)
        {
            this.tp_elem = tp_el;
            sequence = new UniversalSequenceBase(tp_el, streamGen());
        }
        // Без индексов можно последовательноть очистить, загрузить поток значений, сканировать поток значений
        public void Clear()
        {
            sequence.Clear();
        }

        public long Count() { return sequence.Count(); }

        public void Load(IEnumerable<object> flow)
        {
            Clear();
            foreach (object element in flow)
            {
                sequence.AppendElement(element);
            }
            sequence.Flush();
            Build();
        }
        public void Build()
        { 
            foreach (var index in Indexes) index.Build();
        }
        public void Flush() { sequence.Flush(); }
        public void Refresh() { sequence.Refresh(); }
        public IEnumerable<object> ElementValues()
        {
            return sequence.ElementValues();
        }
        public void Scan(Func<long, object, bool> handler)
        {
            sequence.Scan(handler);
        }
        public object GetItem(long off) { return sequence.GetElement(off); }

        public long AddItem(object item)
        {
            long off = sequence.AppendElement(item);
            sequence.Flush();
            // Запуск хендлеров ...
            foreach (var index in Indexes) index.OnAddItem(item, off);
            return off;
        }
        public void DeleteItem(long off) { throw new Exception("Not implemented"); }

        public IIndex[] Indexes { get; set; }
    }
}
