﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Polar.DB;

namespace TripleStore
{
    public class TripleStoreInt32
    {
        private Nametable32 nt;
        //private UniversalSequenceBase table;
        private BearingDeletable table;
        private IndexKey32Comp s_index;
        private IndexKey32Comp inv_index;
        //private IndexKey32Imm i_index;
        private IndexView name_index;
        private Comparer<object> comp_like;
        public TripleStoreInt32(Func<Stream> stream_gen, string tmp_dir_path)
        {
            // сначала таблица имен
            nt = new Nametable32(stream_gen);
            // Тип Object Variants
            PType tp_ov = new PTypeUnion(
                new NamedType("dummy", new PType(PTypeEnumeration.none)),
                new NamedType("iri", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring))
                //new NamedType("int", new PType(PTypeEnumeration.sstring)),
                //new NamedType("date", new PType(PTypeEnumeration.sstring)),
                //new NamedType("langstr", new PTypeRecord(
                //    new NamedType("lang", new PType(PTypeEnumeration.sstring)),
                //    new NamedType("str", new PType(PTypeEnumeration.sstring))))
                    );
            PType tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            // Главная последовательность кодированных триплетов
            //table = new UniversalSequenceBase(tp_triple, stream_gen());
            table = new BearingDeletable(tp_triple, stream_gen);

            // прямой ссылочный индекс
            s_index = new IndexKey32Comp(stream_gen, table, ob => true,
                ob => (int)((object[])ob)[0], null);

            // Обратный ссылочный индекс
            inv_index = new IndexKey32Comp(stream_gen, table,
                ob => (int)((object[])((object[])ob)[2])[0] == 1,
                obj =>
                {
                    object[] pair = (object[])((object[])obj)[2];
                    return (int)pair[1];
                }, null);

            // Индекс по тексту объектов триплетов с предикатом http://fogid.net/o/name
            int p_name = Int32.MinValue;
            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                return string.Compare(
                    (string)((object[])((object[])a)[2])[1], 
                    (string)((object[])((object[])b)[2])[1], StringComparison.OrdinalIgnoreCase);
            }));
            int cod_name = nt.GetSetStr("http://fogid.net/o/name");
            name_index = new IndexView(stream_gen, table, ob => (int)((object[])ob)[1] == cod_name, comp, tmp_dir_path, 20_000_000);

            comp_like = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                int len = ((string)((object[])((object[])b)[2])[1]).Length;
                return string.Compare(
                    (string)((object[])((object[])a)[2])[1], 0, 
                    (string)((object[])((object[])b)[2])[1], 0, len, StringComparison.OrdinalIgnoreCase);
            }));

            table.Indexes = new IIndex[] { s_index, inv_index, name_index };
        }

        public void Build(IEnumerable<object> triples)
        {
            Load(triples);
            s_index.Build();
            //i_index.Build();
            inv_index.Build();
            name_index.Build();
            nt.Build();
            nt.Flush();
        }
        private void Load(IEnumerable<object> triples)
        {
            table.Clear();
            nt.Clear();
            foreach (object tri in triples)
            {
                //long off = table.AppendElement(tri);
                table.AddItem(tri);
            }
            table.Flush();
            nt.Flush();
        }
        public void Refresh()
        {
            s_index.Refresh();
            table.Refresh();
            inv_index.Refresh();
            name_index.Refresh();
            nt.Refresh();
        }
        public IEnumerable<object> Get_s(int subj)
        {
            return s_index.GetAllBySample(new object[] { subj, -1, null });
        }
        public IEnumerable<object> Get_s(string subj_str)
        {
            if (!TryGetCode(subj_str, out int subj)) return Enumerable.Empty<object>();
            return s_index.GetAllBySample(new object[] { subj, -1, null });
        }
        public IEnumerable<object> Get_t(int obj)
        {
            return inv_index.GetAllByKey(obj);
        }
        public IEnumerable<object> Get_t(string obj_str)
        {
            if (!TryGetCode(obj_str, out int obj)) return Enumerable.Empty<object>();
            return inv_index.GetAllByKey(obj);
        }
        public IEnumerable<object> Like(string sample)
        {
            return name_index.SearchAll(new object[] { -1, -1, new object[] { 2, sample } }, comp_like);
        }

        // ================== Утилиты ====================
        internal object[] CodeTriple(object[] tri)
        {
            int subj = nt.GetSetStr((string)tri[0]);
            int pred = nt.GetSetStr((string)tri[1]);
            int tg = (int)((object[])tri[2])[0];
            if (tg == 1)
            {
                int oobj = nt.GetSetStr((string)((object[])tri[2])[1]);
                return new object[] { subj, pred, new object[] { 1, oobj } };
            }
            else
            {
                string dobj = (string)((object[])tri[2])[1];
                return new object[] { subj, pred, new object[] { 2, dobj } };
            }
        }
        internal string TripleToString(object[] tr)
        {
            string subj = nt.Decode((int)tr[0]);
            string pred = nt.Decode((int)tr[1]);
            int tg = (int)((object[])tr[2])[0];
            object v = ((object[])tr[2])[1];
            return "<" + subj + "> <" + pred + "> " +
                (tg == 1 ? "<" + nt.Decode((int)v) + ">" : "\"" + (string)v + "\"") +
                " .";
        }

        public bool TryGetCode(string s, out int code)
        {
            return nt.TryGetCode(s, out code);
        }

    }

}
