using System;
using System.Collections.Generic;
using System.Linq;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class IndexKeyImmutable<Tkey> : IIndexImmutable<Tkey> where Tkey : IComparable
    {
        private PaCell index_cell;
        public PaCell IndexCell { get { return index_cell; } }
        public IndexKeyImmutable(System.IO.Stream stream)
        {
            Type typ = typeof(Tkey);
            if (typ != typeof(int) && typ != typeof(long)) throw new Exception("Err: wrong type of key");
            PType tp_key = typ == typeof(int) ? new PType(PTypeEnumeration.integer) : new PType(PTypeEnumeration.longinteger);
            PType tp_index = new PTypeSequence(new PTypeRecord(
                new NamedType("key", tp_key),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))));
            index_cell = new PaCell(tp_index, stream, false);
        }

        public IndexKeyImmutable(System.IO.Stream stream, IBearingTable table, Func<object, Tkey> keyProducer, IScale scale) : this(stream)
        {
            Table = table;
            KeyProducer = keyProducer;
            Scale = scale;
            scale.IndexCell = index_cell;
        }
        public IndexKeyImmutable(string path_name)
        {
            Type typ = typeof(Tkey);
            if (typ != typeof(int) && typ != typeof(long)) throw new Exception("Err: wrong type of key");
            PType tp_key = typ == typeof(int) ? new PType(PTypeEnumeration.integer) :  new PType(PTypeEnumeration.longinteger);
            PType tp_index = new PTypeSequence(new PTypeRecord(
                new NamedType("key", tp_key),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))));
            index_cell = new PaCell(tp_index, path_name + ".pac", false);
        }

        public IndexKeyImmutable(string path_name, IBearingTable table, Func<object, Tkey> keyProducer, IScale scale) :this(path_name)
        {
            Table = table;
            KeyProducer = keyProducer;
            Scale = scale;
            scale.IndexCell = index_cell;
        }
        public Func<object, Tkey> KeyProducer { get; set; }
        public IBearingTable Table { get; set; }
        public void FillPortion(IEnumerable<TableRow> tableRows)
        {
            foreach (var tableRow in tableRows)
            {
                var key = KeyProducer(tableRow.Row);
                index_cell.Root.AppendElement(new object[] {key, tableRow.Offset});
            }
        }

        public void FillFinish()
        {
            index_cell.Flush();
        }

        public void FillInit()
        {
            index_cell.Clear();
            index_cell.Fill(new object[0]);
            if (KeyProducer == null) throw new Exception("Err: KeyProducer not defined");
        }

        public IScale Scale { get; set; }
        public IEnumerable<PaEntry> GetAllByLevel(Func<PaEntry, int> levelFunc)
        {
            throw new NotImplementedException();
        }

        public void Build()
        {

            index_cell.Clear();
            index_cell.Fill(new object[0]);
            if (KeyProducer == null) throw new Exception("Err: KeyProducer not defined");
            Table.Scan((offset, o) =>
            {
                var key = KeyProducer(o);
                index_cell.Root.AppendElement(new object[] { key, offset });
                return true;
            });
            index_cell.Flush();
            if (index_cell.Root.Count() == 0) return; // потому что следующая операция не пройдет
            index_cell.Root.SortByKey<Tkey>((object v) =>
            {
                var vv = (Tkey)(((object[])v)[0]);
                return vv;
            });
            if (Scale != null) Scale.Build();
        }
        public void Warmup() { foreach (var v in index_cell.Root.ElementValues()); }
        public void ActivateCache() { index_cell.ActivateCache(); if (Scale != null) Scale.ActivateCache(); }

        public IEnumerable<PaEntry> GetAllByKey(long start, long number, Tkey key)
        {
            if (Table == null || Table.Count() == 0) return Enumerable.Empty<PaEntry>();
            PaEntry entry = Table.Element(0);
            PaEntry entry1 = entry;
            var query = index_cell.Root.BinarySearchAll(start, number, ent =>
            {
                var ob = (Tkey)(ent.Field(0).Get());
                return ob.CompareTo(key);
            });
            return query.Select(ent =>
            {
                entry1.offset = (long)ent.Field(1).Get();
                return entry1;
            });
        }

        public IEnumerable<PaEntry> GetAllByKey(Tkey key)
        {
            if (Scale != null)
            {
                Diapason dia = Scale.GetDiapason(Convert.ToInt32(key));
                if (dia.numb == 0) return Enumerable.Empty<PaEntry>();
                else return GetAllByKey(dia.start, dia.numb, key);
            }
            return GetAllByKey(0, index_cell.Root.Count(), key);
        }
        public long Count() { return index_cell.Root.Count(); }
    }
}
