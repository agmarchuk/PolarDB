using System;
using System.Collections.Generic;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class TableViewImmutable : IBearingTableImmutable
    {
        
        protected PaCell table_cell;
        public TableViewImmutable() { /*throw new Exception("Err: wrong constructor for TableViewImmutable");*/ }
        public TableViewImmutable(System.IO.Stream stream, PType e_type)
        {
            table_cell = new PaCell(new PTypeSequence(e_type), stream, false);
        }
        public TableViewImmutable(string path_name, PType e_type)
        {
            table_cell = new PaCell(new PTypeSequence(e_type), path_name + ".pac", false);
        }

        public void Clear()
        {
            table_cell.Clear();
            table_cell.Fill(new object[0]);
        }
        //public virtual void ActivateCache() { table_cell.ActivateCache(); }

        public virtual void Fill(IEnumerable<object> elements)
        {
            Clear();
            foreach (var el in elements) table_cell.Root.AppendElement(el);
            table_cell.Flush();
        }
        public TableViewImmutable FillTV(IEnumerable<object> elements)
        {
            Fill(elements);
            return this;
        }

        public void Scan(Func<long, object, bool> doit)
        {
            table_cell.Root.Scan(doit);
        }

        public PaEntry Element(long ind)
        {
            return table_cell.Root.Element(ind);
        }

        //public object GetValue(long offset)
        //{
        //    PaEntry entry = table_cell.Root.Element(0);
        //    entry.offset = offset;
        //    return entry.Get();
        //}

        public long Count()
        {
            if (table_cell.IsEmpty) return 0L;
            return table_cell.Root.Count();
        }

        // Традиционный способ загрузки значениями
        public long AppendElement(object rec_obj_value)
        {
            var off = table_cell.Root.AppendElement(rec_obj_value);
            return off;
        }
        public void Flush() { table_cell.Flush(); }
    }
}
