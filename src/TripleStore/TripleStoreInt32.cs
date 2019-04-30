using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Polar.DB;

namespace TripleStore
{
    public class TripleStoreInt32
    {
        private Nametable32 nt;
        private UniversalSequenceBase table;
        private IndexKey32CompImmutable s_index;
        private IndexKey32CompImmutable inv_index;
        //private IndexKey32Imm i_index;
        private IndexViewImmutable name_index;
        private Comparer<object> comp_like;
        public TripleStoreInt32(Func<Stream> stream_gen, string tmp_dir_path)
        {
            // сначала таблица имен
            nt = new Nametable32(stream_gen);
            // Тип Object Variants
            PType tp_ov = new PTypeUnion(
                new NamedType("dummy", new PType(PTypeEnumeration.none)),
                new NamedType("iri", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)),
                new NamedType("int", new PType(PTypeEnumeration.sstring)),
                new NamedType("date", new PType(PTypeEnumeration.sstring)),
                new NamedType("langstr", new PTypeRecord(
                    new NamedType("lang", new PType(PTypeEnumeration.sstring)),
                    new NamedType("str", new PType(PTypeEnumeration.sstring)))));
            PType tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.integer)),
                new NamedType("pred", new PType(PTypeEnumeration.integer)),
                new NamedType("obj", tp_ov));
            // Главная последовательность кодированных триплетов
            table = new UniversalSequenceBase(tp_triple, stream_gen());

            // прямой ссылочный индекс
            s_index = new IndexKey32CompImmutable(stream_gen, table,
                ob => Enumerable.Repeat<int>((int)((object[])ob)[0], 1), null);

            // Обратный ссылочный индекс
            //i_index = new IndexKey32Imm(stream_gen, table, obj =>
            //{
            //    object[] pair = (object[])((object[])obj)[2];
            //    int tg = (int)pair[0];
            //    if (tg != 1) return Enumerable.Empty<int>();
            //    return Enumerable.Repeat<int>((int)pair[1], 1);
            //}, null);
            inv_index = new IndexKey32CompImmutable(stream_gen, table, obj =>
            {
                object[] pair = (object[])((object[])obj)[2];
                int tg = (int)pair[0];
                if (tg != 1) return Enumerable.Empty<int>();
                return Enumerable.Repeat<int>((int)pair[1], 1);
            }, null);

            // Индекс по тексту объектов триплетов с предикатом http://fogid.net/o/name
            int p_name = Int32.MinValue;
            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                return string.Compare(
                    (string)((object[])((object[])a)[2])[1], 
                    (string)((object[])((object[])b)[2])[1], StringComparison.OrdinalIgnoreCase);
            }));
            name_index = new IndexViewImmutable(stream_gen, table, comp, tmp_dir_path, 20_000_000)
            {
                Filter = obj =>
                {
                    //TODO: не учтен вариант отсутствия константы в 
                    if (p_name == Int32.MinValue)
                    {
                        // Если нет константы, то фильтр не будет пропускать
                        if (!TryGetCode("http://fogid.net/o/name", out p_name)) return false;
                    }
                    int p = (int)((object[])obj)[1];
                    //object[] o = (object[])((object[])obj)[2];
                    //int tg = (int)o[0];
                    if (p == p_name) return true;
                    return false;
                }
            };

            comp_like = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                int len = ((string)((object[])((object[])b)[2])[1]).Length;
                return string.Compare(
                    (string)((object[])((object[])a)[2])[1], 0, 
                    (string)((object[])((object[])b)[2])[1], 0, len, StringComparison.OrdinalIgnoreCase);
            }));

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
                long off = table.AppendElement(tri);
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
        // Генерация потока триплетов
        internal IEnumerable<object> GenerateTripleFlow(IEnumerable<object> triples)
        {
            foreach (object[] tri in triples)
            {
                int subj = nt.GetSetStr((string)tri[0]);
                int pred = nt.GetSetStr((string)tri[1]);
                int tg = (int)((object[])tri[2])[0];
                if (tg == 1)
                {
                    int oobj = nt.GetSetStr((string)((object[])tri[2])[1]);
                    yield return new object[] { subj, pred, new object[] { 1, oobj } };
                }
                else
                {
                    string dobj = (string)((object[])tri[2])[1];
                    yield return new object[] { subj, pred, new object[] { 2, dobj } };
                }
            }
        }
        internal string DecodeTriple(object[] tr)
        {
            string subj = nt.Decode((int)tr[0]);
            string pred = nt.Decode((int)tr[1]);
            int tg = (int)((object[])tr[2])[0];
            object v = ((object[])tr[2])[1];
            return "<" + subj + "> <" + pred + "> " +
                (tg == 1 ? "<" + nt.Decode((int)v) + ">" : "\"" + (string)v + "\"") +
                " .";
        }

        //internal string ToStr(object obj)
        //{
        //    object[] tri = (object[])obj;
        //    object[] ooo = (object[])tri[2];
        //    int tg = (int)ooo[0];
        //    return "<" + nt.Decode((int)tri[0]) + "> <" + nt.Decode((int)tri[1]) + "> " +
        //    (tg == 1 ? "<" + nt.Decode((int)ooo[1]) + ">" : "\"" + ooo[1] + "\"") +
        //    " .";
        //}
        public bool TryGetCode(string s, out int code)
        {
            return nt.TryGetCode(s, out code);
        }

    }

}
