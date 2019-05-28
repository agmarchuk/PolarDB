using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Polar.DB;

namespace TripleStore
{
    /// <summary>
    /// Таблица имен с 32-разрядной базой кодирования и хеширования. Таблица сопоставляет строке целочисленный (32-разряда)
    /// код. Можно добавлять строки, можно по строке получать код, можно по коду получать строку.
    /// </summary>
    public class Nametable32
    {
        // Носителем таблицы является последовательность пар {код, строка}. Номер строки - ее код. Это первично. 
        // По коду строка определяется однозначно (как вводили), по строке код может определяться с учетом эквивалентностей.
        // Вначале таблица пустая, она заполняется 
        private BearingPure table;
        private UniversalSequenceBase str_offsets;

        private IndexKey32Comp name_index;
        //private Dictionary<string, int> dyna_index;
        public Nametable32(Func<Stream> stream_gen)
        {
            PType tp_elem = new PTypeRecord(
                new NamedType("code", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)));
            table = new BearingPure(tp_elem, stream_gen);
            str_offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), stream_gen());
            Comparer<object> comp_str = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                var aa = (string)((object[])a)[1];
                var bb = (string)((object[])b)[1];
                return aa.CompareTo(bb);
            }));

            name_index = new IndexKey32Comp(stream_gen, table, 
                ob => true,
                ob => Hashfunctions.HashRot13((string)((object[])ob)[1]), comp_str);

            table.Indexes = new IIndex[] { name_index };
            //dyna_index = new Dictionary<string, int>();
        }
        public void Clear()
        {
            table.Clear();
            str_offsets.Clear();
            name_index.Clear();
            //dyna_index = new Dictionary<string, int>();
        }
        //public void Load(IEnumerable<string> flow)
        //{
        //    // а правильно ли без очистки name_index ?
        //    int cod = (int)table.Count();
        //    foreach (string s in flow)
        //    {
        //        long off = table.AppendElement(new object[] { cod, s });
        //        cod++;
        //        str_offsets.AppendElement(off);
        //    }
        //    table.Flush();
        //    str_offsets.Flush();
        //}
        public void Build()
        {
            // Про порядок операторов еще надо подумать
            //dyna_index = new Dictionary<string, int>();
            name_index.Build();
        }
        public void Refresh()
        {
            table.Refresh();
            str_offsets.Refresh();
            name_index.Refresh();
        }
        // ==================== Динамика ===================
        private int SetStr(string s)
        {
            int code = (int)table.Count();
            long off = table.AddItem(new object[] { code, s });
            str_offsets.AppendElement(off);
            //dyna_index.Add(s, code);
            // нужен итоговый Flush по двум последовательностям
            return code;
        }
        public void Flush()
        {
            table.Flush();
            str_offsets.Flush();
        }
        public bool TryGetCode(string s, out int code)
        {
            //if (dyna_index.TryGetValue(s, out code)) return true;
            code = -1;
            var q = name_index.GetAllBySample(new object[] { -1, s }).FirstOrDefault(ob => (string)((object[])ob)[1] == s);
            if (q == null) return false;
            code = (int)((object[])q)[0];
            return true;
        }
        public int GetSetStr(string s)
        {
            int code;
            if (TryGetCode(s, out code)) return code;
            code = SetStr(s);
            return code;
        }
        public string Decode(int cod)
        {
            long off = (long)str_offsets.GetByIndex(cod);
            return (string)((object[])table.GetItem(off))[1];
        }
    }
}
