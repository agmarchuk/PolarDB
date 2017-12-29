using System;
using System.Collections.Generic;
using System.Linq;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class IndexHalfkeyImmutable<Tkey> : IIndexImmutable<Tkey> where Tkey : IComparable
    {
        private PaCell index_cell;
        public PaCell IndexCell { get { return index_cell; } set { index_cell = value; } }
        public IndexHalfkeyImmutable(System.IO.Stream stream)
        {
            PType tp_hkey = new PType(PTypeEnumeration.integer);
            PType tp_index = new PTypeSequence(new PTypeRecord(
                new NamedType("halfkey", tp_hkey),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))));
            index_cell = new PaCell(tp_index, stream, false);
        }
        public IndexHalfkeyImmutable(string path_name)
        {
            PType tp_hkey = new PType(PTypeEnumeration.integer);
            PType tp_index = new PTypeSequence(new PTypeRecord(
                new NamedType("halfkey", tp_hkey),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))));
            index_cell = new PaCell(tp_index, path_name + ".pac", false);
        }
        public Func<object, Tkey> KeyProducer { get; set; }
        public Func<Tkey, int> HalfProducer { get; set; }
        public IBearingTable Table { get; set; }
        public IScale Scale { get; set; }
        public IEnumerable<PaEntry> GetAllByLevel(Func<PaEntry, int> levelFunc)
        {
            throw new NotImplementedException();
        }

        public class HalfPair : IComparable, IComparer<Tkey>
        {
            private long record_off;
            private int hkey;
            private IndexHalfkeyImmutable<Tkey> index;
            private bool key_exists = false;
            private Tkey _key;
            public Tkey Key
            {
                get
                {
                    if (!key_exists)
                    {
                        PaEntry entry = index.Table.Element(0);
                        entry.offset = this.record_off;
                        _key = index.KeyProducer((object[])entry.Get());
                        key_exists = true;
                    }
                    return _key;
                }
            }
            public HalfPair(long rec_off, int hkey, IndexHalfkeyImmutable<Tkey> index)
            {
                this.record_off = rec_off; this.hkey = hkey; this.index = index;
            }
            public int CompareTo(object pair)
            {
                if (!(pair is HalfPair)) throw new Exception("Exception 284401");
                HalfPair pa = (HalfPair)pair;
                int cmp = this.hkey.CompareTo(pa.hkey);
                if (cmp != 0) return cmp;
                //if (index.Table.Count() == 0) throw new Exception("Ex: 2943991");
                // Определяем ключ 
                //PaEntry entry = index.Table.Element(0);
                //entry.offset = pa.record_off;
                //Tkey key = index.KeyProducer((object[])entry.Get());
                //entry.offset = record_off;
                //return index.KeyProducer((object[])entry.Get()).CompareTo(key);
                return this.Key.CompareTo(pa.Key);
            }
            public int Compare(Tkey x, Tkey y)
            {
                return x.CompareTo(y);
            }
        }

        public void Build2()
        {
            index_cell.Clear();
            index_cell.Fill(new object[0]);
            if (KeyProducer == null) throw new Exception("Err: KeyProducer not defined");
            Table.Scan((offset, o) =>
            {
                var key = KeyProducer(o);
                int hkey = (int)HalfProducer(key);
                index_cell.Root.AppendElement(new object[] { hkey, offset });
                return true;
            });
            index_cell.Flush();
            if (index_cell.Root.Count() == 0) return; // потому что следующая операция не пройдет


            var ptr = Table.Element(0);
            index_cell.Root.SortByKey<HalfPair>((object v) =>
            {
                object[] vv = (object[])v;
                object half_key = vv[0];
                long offset = (long)vv[1];
                ptr.offset = offset;
                return new HalfPair(offset, (int)half_key, this);
            });
            index_cell.Flush();

            if (Scale != null) Scale.Build();
        }
        public void Build()
        {
            index_cell.Clear();
            index_cell.Fill(new object[0]);
            if (KeyProducer == null) throw new Exception("Err: KeyProducer not defined");
            Table.Scan((offset, o) =>
            {
                var key = KeyProducer(o);
                int hkey = (int)HalfProducer(key);
                index_cell.Root.AppendElement(new object[] { hkey, offset });
                return true;
            });
            index_cell.Flush();
            if (index_cell.Root.Count() == 0) return; // потому что следующая операция не пройдет
            var ptr = Table.Element(0);
            index_cell.Root.SortByKey<HalfPair>((object v) =>
            {
                object[] vv = (object[])v;
                object half_key = vv[0];
                long offset = (long)vv[1];
                ptr.offset = offset;
                return new HalfPair(offset, (int)half_key, this);
            });

            if (Scale != null) Scale.Build();
        }
        public void BuildScale() { Scale.Build(); }
        public void StatisticsAfterSorting()
        {
            long total = index_cell.Root.Count(); 
            long different = 0;
            int current = Int32.MinValue;
            foreach (object[] pair in index_cell.Root.ElementValues())
            {
                int hkey = (int)pair[0];
                if (hkey > current) { different++; }
                else if (hkey == current) { }
                else throw new Exception("Error in sort order");
                current = hkey;
            }
            Console.WriteLine("Statistics: total={0} different={1}", total, different); 
        }
        public void Statistics()
        {
            long total = index_cell.Root.Count();
            long conflicts = 0;
            Dictionary<int, int> hkeyUsed = new Dictionary<int, int>();
            foreach (object[] pair in index_cell.Root.ElementValues())
            {
                int hkey = (int)pair[0];
                if (hkeyUsed.ContainsKey(hkey))
                {
                    conflicts++;
                }
                else
                {
                    hkeyUsed.Add(hkey, 1);
                }
            }
            Console.WriteLine("Statistics: total={0} different={1}", total, hkeyUsed.Count);
        }

        public void Warmup() { foreach (var v in index_cell.Root.ElementValues()); if (Scale != null) Scale.Warmup(); }
        public void ActivateCache() { index_cell.ActivateCache(); if (Scale != null) Scale.ActivateCache(); }

        public IEnumerable<PaEntry> GetAllByKey(Tkey key)
        {
            if (Scale != null)
            {
                Diapason dia = Scale.GetDiapason(HalfProducer(key));
                if (dia.numb == 0) return Enumerable.Empty<PaEntry>();
                else if (dia.numb < 200) return GetAllByKey2(dia.start, dia.numb, key);
                else return GetAllByKey(dia.start, dia.numb, key);
            }
            return GetAllByKey(0, index_cell.Root.Count(), key);
        }
        public IEnumerable<PaEntry> GetAllByKey(long start, long number, Tkey key)
        {
            if (Table == null || Table.Count() == 0) return Enumerable.Empty<PaEntry>();
            PaEntry entry = Table.Element(0);
            PaEntry entry1 = entry;
            int hkey = HalfProducer(key);
            var qqq = index_cell.Root.Elements().Take(100).ToArray();
            var candidates = index_cell.Root.BinarySearchAll(start, number, ent =>
            {
                object[] pair = (object[])ent.Get();
                int hk = (int)pair[0];
                int cmp = hk.CompareTo(hkey);
                if (cmp != 0) return cmp;
                long off = (long)pair[1];
                entry.offset = off;
                return ((IComparable)KeyProducer((object[])entry.Get())).CompareTo(key);
            }).ToArray();
            return candidates.Select(ent =>
            {
                entry1.offset = (long)ent.Field(1).Get();
                return entry1;
            });
        }
        public IEnumerable<PaEntry> GetAllByKey2(long start, long number, Tkey key)
        {
            if (Table == null || Table.Count() == 0) return Enumerable.Empty<PaEntry>();
            PaEntry entry = Table.Element(0);
            PaEntry entry1 = entry;
            int hkey = HalfProducer(key);

            var entries = index_cell.Root.ElementValues(start, number)
                .Cast<object[]>()
                .TakeWhile(va => (int)va[0] <= hkey)
                .Where(va => (int)va[0] == hkey)
                .Select(va => { long off = (long)va[1]; entry.offset = off; return entry; })
                .Where(va =>
                {
                    var ka = KeyProducer(entry.Get());
                    return ka.CompareTo(key) == 0;
                });
            return entries;
        }

        public long Count() { return index_cell.Root.Count(); }

        public void FillPortion(IEnumerable<TableRow> rows)
        {
            foreach (var row in rows)
            {
                var key = KeyProducer(row.Row);
                int hkey = (int)HalfProducer(key);
                index_cell.Root.AppendElement(new object[] { hkey, row.Offset});
            }
        }

        public void FillInit()
        {
            index_cell.Clear();
            index_cell.Fill(new object[0]);
        }

        public void FillFinish()
        {
            index_cell.Flush();
        }
       
    }
}
