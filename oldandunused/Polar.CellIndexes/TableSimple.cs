using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.CellIndexes
{
    public class TableSimple
    {
        private PType tp_element;
        private Func<Stream> getstream;
        private TableView table;
        public TableSimple(PType tp_elem, int[] indexcolumnnoms, Func<Stream> getstream)
        {
            this.tp_element = tp_elem;
            this.getstream = getstream;
            this.cnoms = indexcolumnnoms;
            table = new TableView(getstream(), tp_elem);
            if (tp_element.Vid != PTypeEnumeration.record) throw new Exception("Err in TableRelational Indexes: Type of element is not record");
            allindexes = new IIndexCommon[cnoms.Length];
            PTypeRecord tp_r = (PTypeRecord)tp_element;
            int i = 0;
            foreach (int nom in cnoms)
            {
                if (nom < 0 || nom > tp_r.Fields.Length) throw new Exception("Err in TableRelational Indexes: element number is out of range");
                PType tp = tp_r.Fields[nom].Type;
                if (tp.Vid == PTypeEnumeration.integer)
                {
                    Func<object, int> keyproducer = v => (int)((object[])((object[])v)[1])[nom];
                    IndexKeyImmutable<int> ind_arr = new IndexKeyImmutable<int>(getstream())
                    {
                        Table = table,
                        KeyProducer = keyproducer,
                        Scale = null
                    };
                    ind_arr.Scale = new ScaleCell(getstream()) { IndexCell = ind_arr.IndexCell };
                    IndexDynamic<int, IndexKeyImmutable<int>> index = new IndexDynamic<int, IndexKeyImmutable<int>>(true, ind_arr);
                    allindexes[i] = index;
                    table.RegisterIndex(index);
                }
                else if (tp.Vid == PTypeEnumeration.sstring)
                {
                    IndexHalfkeyImmutable<string> index_arr = new IndexHalfkeyImmutable<string>(getstream())
                    {
                        Table = table,
                        KeyProducer = v => (string)((object[])((object[])v)[1])[nom],
                        HalfProducer = v => Hashfunctions.HashRot13(v)
                    };
                    index_arr.Scale = new ScaleCell(getstream()) { IndexCell = index_arr.IndexCell };
                    IndexDynamic<string, IndexHalfkeyImmutable<string>> index =
                        new IndexDynamic<string, IndexHalfkeyImmutable<string>>(false, index_arr);
                    allindexes[i] = index;
                    table.RegisterIndex(index);
                }
                else throw new Exception($"Err: vid {tp.Vid} is not implemented in TableRelational Indexes");
                i++;
            }
        }
        private int[] cnoms;
        private IIndexCommon[] allindexes;
        //public void Indexes(int[] cnoms)
        //{
        //}
        public void Fill(IEnumerable<object> flow)
        {
            table.ClearIndexes(); table.Clear();
            table.Fill(flow);
            table.BuildIndexes();
        }
        public IEnumerable<object[]> GetAllByKey(int column, object key)
        {
            int i;
            for (i = 0; i < cnoms.Length; i++) if (cnoms[i] == column) break;
            if (i == cnoms.Length) throw new Exception("Err: 938948");

            var index = allindexes[i];
            if (index is IndexDynamic<int, IndexKeyImmutable<int>>)
            {
                IndexDynamic<int, IndexKeyImmutable<int>> ind = (IndexDynamic<int, IndexKeyImmutable<int>>)index;
                return ind.GetAllByKey((int)key).Select(ent => ((object[])ent.Get())[1]).Cast<object[]>();
            }
            else if (index is IndexDynamic<string, IndexHalfkeyImmutable<string>>)
            {
                IndexDynamic<string, IndexHalfkeyImmutable<string>> ind = (IndexDynamic<string, IndexHalfkeyImmutable<string>>)index;
                return ind.GetAllByKey((string)key).Select(ent => ((object[])ent.Get())[1]).Cast<object[]>();
            }
            return Enumerable.Empty<object[]>();
        }
        public IEnumerable<object[]> ElementValues()
        {
            return table.ElementValues().Cast<object[]>();
        }
        public long Count()
        {
            return table.Count();
        }
        public void AppendValue(object[] row)
        {
            table.AppendValue(row);
        }
    }
}
