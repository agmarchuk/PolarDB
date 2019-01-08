using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Polar.DB;

namespace Universal
{
    public class IndexKey32Immutable : IIndex //IIndexImmutable<Tkey> where Tkey : IComparable
    {
        private bool keyisint = false;
        private UniversalSequenceBase index_arr;
        private Func<object, int> keyProducer;
        private Func<Stream> streamGen;
        public Sequence BearingSequence { get; set; }
        public IndexKey32Immutable(Func<object, int> keyProducer, Func<Stream> streamGen)
        {
            this.keyProducer = keyProducer;
            this.streamGen = streamGen;
            keyisint = true;
            PType tp_key = new PType(PTypeEnumeration.integer);
            PType tp_index_el = new PTypeRecord(
                new NamedType("key", tp_key),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger)));
            index_arr = new UniversalSequenceBase(tp_index_el, streamGen());
        }

        public void Clear() => index_arr.Clear();
        public void AppendPosition(long offset, object element)
        {
            index_arr.AppendElement(new object[] { keyProducer(element), offset });
        }
        public void Flush()
        {
            index_arr.Flush();
        }
        public void Build32()
        {
            index_arr.Sort32(ob => { var k = keyProducer(ob); return (int)k; });
        }
        //public void Build32_()
        //{
        //    int nelements = (int)index_arr.Count(); //TODO: для массивов это не существенно
        //    int[] keys = new int[nelements];
        //    long[] offsets = new long[nelements];

        //    int nom = 0;
        //    index_arr.Scan((off, ob) =>
        //    {
        //        object okey = keyProducer(ob);
        //        int key = (int)okey;
        //        keys[nom] = key;
        //        offsets[nom] = off;
        //        nom++;
        //        return true;
        //    });
        //    Array.Sort(keys, offsets);
        //    index_arr.Clear();

        //    index_arr.Flush();
        //}

        public void Build()
        {
            Build32();
        }

    }

}
