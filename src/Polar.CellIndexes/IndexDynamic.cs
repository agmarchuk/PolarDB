using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class IndexDynamic<Tkey, IndexImmut> : IIndex<Tkey> where IndexImmut : IIndexImmutable<Tkey>
    {
        //TODO: Экономнее было бы обойтись длинными offset'ами: Dictionary<Tkey, long>. Но откуда брать тип и ячейку для конструирования? Или надо возвращать также офсеты?
        private Dictionary<Tkey, PaEntry> keyent = new Dictionary<Tkey, PaEntry>(); // для уникального
        private Dictionary<Tkey, List<PaEntry>> keyents = new Dictionary<Tkey, List<PaEntry>>(); // стандартно
        public void OnAppendElement(PaEntry entry)
        {
            Tkey key = KeyProducer(entry.Get());
            if (_unique) keyent.Add(key, entry); // Надо бы что-то проверить...
            else
            {
                List<PaEntry> entset;
                if (keyents.TryGetValue(key, out entset))
                {
                    entset.Add(entry);
                }
                else
                {
                    keyents.Add(key, Enumerable.Repeat<PaEntry>(entry, 1).ToList());
                }
            }
        }

        public PaCell IndexCell { get { return null; } } // Этот интерфейс для динамического индекса кажется лишним
        public Func<object, Tkey> KeyProducer { get; set; }
        public IBearingTable Table { get; set; }
        public IndexImmut IndexArray { get; set; } //??
        private bool _unique;
        public bool Unique { get { return _unique; } }
        public IndexDynamic(bool unique)
        {
            this._unique = unique;
        }
        public IndexDynamic(bool unique, IndexImmut index )
        {
            this._unique = unique;
            IndexArray = index;
            Scale = index.Scale;
            KeyProducer = index.KeyProducer;
            Table = index.Table;
        }
        public IScale Scale { get; set; } 
        public long Count() { return IndexArray.Count(); }
        public void Build()
        {
            IndexArray.Build();
            if (_unique) keyent = new Dictionary<Tkey, PaEntry>();
            else keyents = new Dictionary<Tkey, List<PaEntry>>();
        }
        public void Clear()
        {
            keyent = new Dictionary<Tkey, PaEntry>();
            keyents = new Dictionary<Tkey, List<PaEntry>>();
        }
        public void FillPortion(IEnumerable<TableRow> tableRows)
        {
            IndexArray.FillPortion(tableRows);
        }

        public void FillFinish()
        {
            IndexArray.FillFinish();
        }

        public void FillInit()
        {
            IndexArray.FillInit();
        }

        public void Warmup() { IndexArray.Warmup(); }
        public void ActivateCache() { IndexArray.ActivateCache(); }

        public IEnumerable<PaEntry> GetAllByKey(long start, long number, Tkey key)
        {
            throw new Exception("No implementation (no need) of GetAllByKey(long start, long number, Tkey key)");
        }

        //Такой вариант не получается из-за использования шкалы
        //public IEnumerable<PolarDB.PaEntry> GetAllByKey(Tkey key)
        //{
        //    return GetAllByKey(0, IndexArray.Count(), key);
        //}
        public IEnumerable<PaEntry> GetAllByKey(Tkey key)
        {
            if (_unique)
            {
                PaEntry entry;
                if (keyent.TryGetValue(key, out entry))
                {
                    return Enumerable.Repeat<PaEntry>(entry, 1).Concat<PaEntry>(IndexArray.GetAllByKey(key));
                }
                return IndexArray.GetAllByKey(key);
            }
            else
            {
                List<PaEntry> entries;
                if (keyents.TryGetValue(key, out entries))
                {
                    return entries.Concat<PaEntry>(IndexArray.GetAllByKey(key));
                }
                return IndexArray.GetAllByKey(key);
            }
            //return GetAllByKey(0, IndexArray.Count(), key);
        }
        public IEnumerable<PaEntry> GetAllByLevel(Func<Tkey, int> LevelFunc)
        {
            //throw new Exception("GetAllByLevel dois not implemented in DinamicIndexUnique");
            PaEntry[] dyna = new PaEntry[0];
            if (_unique) dyna = keyent.Where(pair => LevelFunc(pair.Key) == 0).Select(pair => pair.Value).ToArray();
            else dyna = keyents.Where(pair => LevelFunc(pair.Key) == 0).SelectMany(pair => pair.Value).ToArray();
            if (Table.Count() == 0) return dyna;
            PaEntry ent = Table.Element(0);
            return dyna.Concat(IndexArray.IndexCell.Root.BinarySearchAll(e => 
            {
                long off = (long)e.Get();
                ent.offset = off;
                Tkey k = KeyProducer(ent.Get());
                return LevelFunc(k);
            }));
        }
        public void DropIndex()
        {
            IndexArray.IndexCell.Clear();
            //Scale.Clear();
            if(_unique)
                keyent.Clear();
            else
                keyents.Clear();
        }

        public IEnumerable<PaEntry> GetAllByLevel(Func<PaEntry, int> levelFunc)
        {
            //TODO dict
            return IndexArray.GetAllByLevel(levelFunc);
            //return GetAllByLevel((Tkey key) => KeyProducer(ent.Get())
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append('{');
            sb.Append('[');
            if (IndexArray.IndexCell.Root.Count() > 0)
            {
                object one = IndexArray.IndexCell.Root.Element(0).Get();
                PaEntry entry = Table.Element(0);
                foreach (object v in IndexArray.IndexCell.Root.ElementValues())
                {
                    long offset;
                    if (one is long)
                    {
                        offset = (long)v;
                    }
                    else
                    {
                        offset = (long)((object[])v)[1];
                    }
                    entry.offset = offset;
                    sb.Append((string)entry.Field(1).Field(1).Get());
                    sb.Append(' ');
                }
            }
            sb.Append(']');
            // динамическая часть
            sb.Append('[');
            if (_unique)
            {
                foreach (PaEntry ent in keyent.Select(pair => pair.Value))
                {
                    sb.Append((string)ent.Field(1).Field(1).Get());
                    sb.Append(' ');
                }
            }
            else
            {
                sb.Append("NOT IMPLEMENTED");
            }
            sb.Append(']');
            sb.Append('}');
            return sb.ToString();
        }
    }
}
