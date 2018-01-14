using System;
using System.Collections.Generic;
using System.Linq;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class TableView : TableViewImmutable, IBearingTable 
    {
        PType tp_rec;
        public TableView() { throw new Exception("Err: wrong constructor for TableView"); }
        public TableView(System.IO.Stream stream, PType e_type)
        {
            tp_rec = new PTypeRecord(
                new NamedType("deleted", new PType(PTypeEnumeration.boolean)),
                new NamedType("evalue", e_type));
            table_cell = new PaCell(new PTypeSequence(tp_rec), stream, false);
        }
        public TableView(string path_name, PType e_type)
        {
            tp_rec = new PTypeRecord(
                new NamedType("deleted", new PType(PTypeEnumeration.boolean)),
                new NamedType("evalue", e_type));
            table_cell = new PaCell(new PTypeSequence(tp_rec), path_name + ".pac", false);
        }
        public override void Fill(IEnumerable<object> values)
        {
            Clear();
            foreach (var index in indexes) index.FillInit();
            List<TableRow> buffer=new List<TableRow>(10000);
            foreach (var el in values)
            {
                if (buffer.Count == 10000)
                {
                    foreach (var index in indexes) index.FillPortion(buffer);
                    buffer.Clear();
                }
                object v = new object[] {false, el};
                long offset = table_cell.Root.AppendElement(v);
                PaEntry entry = new PaEntry(tp_rec, offset, table_cell);
                //foreach (var index in indexes) index.OnAppendElement(entry);
                buffer.Add(new TableRow(v, offset));
            }
            foreach (var index in indexes) index.FillPortion(buffer);
            foreach (var index in indexes) index.FillFinish();

            table_cell.Flush();
        }
        public IEnumerable<PaEntry> Elements() { return table_cell.Root.Elements(); }

        // метод для добавления элементов в таблицу.
        /// <summary>
        /// Добавляет в таблицу объекты, при этом создаётся структура, содержащая объект и offset
        /// </summary>
        /// <param name="values"></param>
        /// <returns>поток пар: объект и offset для добавления в таблици индексов</returns>
        public IEnumerable<TableRow> Add(IEnumerable<object> values)
        {
            return values.Select(el => new object[] {false, el})
                .Select(v => new TableRow(v, table_cell.Root.AppendElement(v)));
        }

                public void Warmup() 
        {
            foreach (var v in table_cell.Root.ElementValues());
            foreach (IIndexCommon index in indexes)
            {
                index.Warmup();
            }
        }
        //public override void ActivateCache()
        //{
        //    table_cell.ActivateCache();
        //}
        List<IIndexCommon> indexes = new List<IIndexCommon>();
        // По имеющейса опорной таблице и коннекторам индексов (в списке indexes), (заново) построить индексы 
        public void BuildIndexes() { foreach (var index in indexes) index.Build(); }
        public void ClearIndexes() { foreach (var index in indexes) index.Clear(); }

        public PaCell TableCell { get { return table_cell; } } // Использование таблицы напрямую требует тонких знаний
        // Целостное действие слабой динамики: ДОбавление элемента в таблицу, фиксация его и вызов хендлеров у индексов
        public PaEntry AppendValue(object value)
        {
            long offset = table_cell.Root.AppendElement(new object[] { false, value });
            //table_cell.Flush();
            PaEntry entry = new PaEntry(tp_rec, offset, table_cell);
            foreach (var index in indexes) index.OnAppendElement(entry);
            return entry;
        }
        /// <summary>
        /// Добавляет порцию без перевычисления индексов
        /// </summary>
        /// <param name="values"></param>
        public void AddPortion(IEnumerable<object> values)
        {
            foreach (var value in values)
            {
                long offset = table_cell.Root.AppendElement(new object[] { false, value });
            }
        }

        public void DeleteEntry(PaEntry record)
        {
            record.Field(0).Set(true);
        }

        public IEnumerable<PaEntry> GetUndeleted(IEnumerable<PaEntry> elements)
        {
            throw new NotImplementedException();
        }
        public void RegisterIndex(IIndexCommon index) 
        {
            indexes.Add(index);
        }
        public void UnregisterIndex(IIndexCommon index) 
        {
            indexes.Remove(index);
        }
    }

}
