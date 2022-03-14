using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.OModel
{
    public class UniversalSequence : IBearing
    {
        internal UniversalSequenceBase usb;
        private Func<object, bool> isEmpty;
        private Func<object, DateTime> timestamp; // Функция может давать минимальное время или то, что зафиксировано в элементе. 
        private UniversalIndex primary_keyIndex;
        private UniversalIndex[] universal_indexes; 

        public UniversalSequence(PType tp_el, Func<Stream> streamGen, Func<object, bool> isEmpty, Func<object, DateTime> timestamp,
            Func<object, int> primary_hashFunc, Comparer<object> primary_comp, HashComp[] indinfo)          
        {
            usb = new UniversalSequenceBase(tp_el, streamGen());
            this.isEmpty = isEmpty;
            this.timestamp = timestamp;
            this.primary_keyIndex = new UniversalIndex(streamGen, this, primary_hashFunc, primary_comp);
            universal_indexes = indinfo.Select(hc => new UniversalIndex(streamGen, this, hc.Hash, hc.Comp)).ToArray();
            Indexes = new IIndex[0];
        }
        // Эти определения можно было бы не делать, работало бы наследование
        public void Clear() 
        { 
            usb.Clear();
            // Чистить индексы
            foreach (var uind in universal_indexes) uind.Clear();
            foreach (var ind in Indexes) ind.Clear();
        }
        public void Flush() { usb.Flush(); }
        public long Count() { return usb.Count(); }

        // Загрузка данных осуществляется обычно в несколько приемов, поэтому обязательны в использовании Clear и Build 
        // до и после серии Load
        public void Load(IEnumerable<object> flow)
        {
            foreach (var obj in flow)
            {
                usb.AppendElement(obj);
            }
            usb.Flush();
        }
        public void Build()
        { 
            primary_keyIndex.Build();
            foreach (var ind in universal_indexes) ind.Build();
            foreach (var iii in Indexes) iii.Build();
        }
        public void Refresh()
        {
            usb.Refresh();
            primary_keyIndex.Refresh();
            foreach (var ind in universal_indexes) ind.Refresh();
        }

        public IEnumerable<object> Elements()
        {  //TODO: НАдо учесть признак пустоты и является ли оригиналом
            return usb.ElementValues();
        }

        public object GetByKey(object sample)
        {
            return primary_keyIndex.GetBySample(sample);
        }
        public IEnumerable<object> GetAllUsingIndex(int indnom, object sample)
        {
            if (indnom < 0 || indnom >= universal_indexes.Length) throw new Exception("Err in GetUsingIndex");
            return universal_indexes[indnom].GetAllBySample(sample);
        }
        public IEnumerable<object> LikeUsingIndex(int indnom, object sample, Comparer<object> comp)
        {
            if (indnom < 0 || indnom >= universal_indexes.Length) throw new Exception("Err in GetUsingIndex");
            return universal_indexes[indnom].Like(sample, comp);
        }

        public void Scan(Func<long, object, bool> handler)
        {
            usb.Scan(handler);
        }
        public object GetItem(long off)
        {
            return usb.GetElement(off);
        }

        public IIndex[] Indexes { get; set; }

        // ============ Это из-за интерфейса IBearing

        public long AddItem(object item)
        {
            throw new NotImplementedException();
        }

        public void DeleteItem(long off)
        {
            throw new NotImplementedException();
        }


        public IEnumerable<object> ElementValues()
        {
            throw new NotImplementedException();
        }

    }
}
