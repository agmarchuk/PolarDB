using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;

using Polar.DB;
//using Polar.TripleStore;

namespace Polar.TripleStore
{
    public class TripleRecordStore
    {
        private Nametable32 nt;
        //private UniversalSequenceBase table;
        private BearingDeletable table;
        private IndexKey32Comp s_index;
        private IndexKey32CompVector inv_index;
        private IndexView name_index;
        private Comparer<object> comp_like;
        private string[] preload_names = { };
        public string[] Preload { get { return preload_names; } set { preload_names = value; LoadPreloadnames(); } }
        private int cod_name;
        internal void LoadPreloadnames()
        {
            foreach (string s in preload_names) nt.GetSetStr(s);
            nt.Flush();
        }

        public int Code(string s) => nt.GetSetStr(s);
        public string Decode(int c) => nt.Decode(c);

        public TripleRecordStore(Func<Stream> stream_gen, string tmp_dir_path, string[] preload_names)
        {
            // сначала таблица имен
            nt = new Nametable32(stream_gen);
            // Предзагрузка должна быть обеспечена даже для пустой таблицы имен
            this.preload_names = preload_names;
            LoadPreloadnames();
            // Тип записи
            PType tp_record = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("directs",
                    new PTypeSequence(new PTypeRecord(
                        new NamedType("prop", new PType(PTypeEnumeration.integer)),
                        new NamedType("entity", new PType(PTypeEnumeration.integer))))),
                new NamedType("fields",
                    new PTypeSequence(new PTypeRecord(
                        new NamedType("prop", new PType(PTypeEnumeration.integer)),
                        new NamedType("value", new PType(PTypeEnumeration.sstring)))))
                );
            // Главная последовательность: множестов кодированных записей
            table = new BearingDeletable(tp_record, stream_gen);
            // прямой ключевой индекс: по задаваемому ключу получаем запись
            s_index = new IndexKey32Comp(stream_gen, table, ob => true,
                ob => (int)((object[])ob)[0], null);
            // Обратный ссылочный индекс
            inv_index = new IndexKey32CompVector(stream_gen, table,
                obj =>
                {
                    object[] directs = (object[])((object[])obj)[1];
                    return directs.Cast<object[]>()
                        .Select(pair => (int)pair[1]);
                }, null);


            // Индекс по текстам полей записей триплетов с предикатом http://fogid.net/o/name
            // Предполагается, что в Preload есть такое имя:
            cod_name = nt.GetSetStr("http://fogid.net/o/name");
            // Это компаратор сортировки. (более ранний комментарий: компаратор надо поменять!!!!!!)
            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                object predval1 = ((object[])((object[])a)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == cod_name);
                object predval2 = ((object[])((object[])b)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == cod_name);

                return string.Compare(
                    (string)((object[])predval1)[1],
                    (string)((object[])predval2)[1],
                    StringComparison.OrdinalIgnoreCase);
            }));

            comp_like = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])((object[])((object[])a)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == cod_name))[1];
                string val2 = (string)((object[])((object[])((object[])b)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == cod_name))[1];
                int len = val2.Length;
                return string.Compare(
                    val1, 0,
                    val2, 0, len, StringComparison.OrdinalIgnoreCase);
            }));
            // name-индекс пока будет скалярным и будет индексировать первое (!) name-поле в записи
            name_index = new IndexView(stream_gen, table,
                ob => ((object[])((object[])ob)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == cod_name) != null, 
                comp, tmp_dir_path, 20_000_000);

            table.Indexes = new IIndex[] { s_index, inv_index, name_index };

        }
        public void Clear()
        {
            table.Clear();
            s_index.Clear();
            inv_index.Clear();
            name_index.Clear();
            nt.Clear();
            // Предзагрузка
            LoadPreloadnames();
        }
        // Специально кодирует объект. Если целый, то это уже код, если строка, то кодирует с помощью Nametable
        private int Cd(object v) => v is int ? (int)v : Code((string)v);
        // Процедуры кодирования и декодирования записей. Используется при вводе (кодирование) и выводе (декодирование) результатов
        public object[] CodeRecord(object[] irec)
        {
            object[] v = new object[] 
            {
                Cd(irec[0]), 
                    ((object[])(irec[1])).Cast<object[]>().Select(pair => new object[] { Cd(pair[0]), Cd(pair[1]) }).ToArray(),
                    ((object[])(irec[2])).Cast<object[]>().Select(pair => new object[] { Cd(pair[0]), pair[1] }).ToArray()
            };
            return v;
        }
        public void Load(IEnumerable<object> records)
        {
            int ii = 0;
            foreach (object[] record in records)
            {
                table.AppendItem(record);
                ii++;
                if (ii % 1_000_000 == 0) Console.Write($"{ii / 1_000_000} ");
            }
            Console.WriteLine("load ok.");
            table.Flush();
        }
        public void Build()
        {
            nt.Build();
            nt.Flush();
            GC.Collect();
            Console.WriteLine("Nametable ok.");
            s_index.Build();
            GC.Collect();
            Console.WriteLine("s_index ok.");
            inv_index.Build();
            GC.Collect();
            Console.WriteLine("inv_index ok.");
            name_index.Build();
            GC.Collect();
            Console.WriteLine("name_index ok.");
        }
        public void Refresh()
        {
            table.Refresh();
            s_index.Refresh();
            inv_index.Refresh();
            name_index.Refresh();
            nt.Refresh();
        }

        public object GetRecord(int c)
        {
            var qu = s_index.GetAllByKey(c)
                .FirstOrDefault();
            return qu;
        } 
        public IEnumerable<object> GetRefers(int c)
        {
            var qu = inv_index.GetAllByKey(c);
            return qu;
        }
        public IEnumerable<object> Like(string sample)
        {
            return name_index.SearchAll(new object[] { -1, new object[0], new object[] { new object[] { cod_name, sample } } }, comp_like);
        }


        // ================== Утилиты ====================
        public int CodeEntity(string en) { return nt.GetSetStr(en); }
        public string DecodeEntity(int ient) { return nt.Decode(ient); }

        public string ToTT(object rec)
        {
            object[] rr = (object[])rec;
            int id = (int)rr[0];
            Func<int, string> Dec = (int c) => c < 0? ""+(-c-1) : nt.Decode(c);
            object[] directs = (object[])rr[1];
            object[] fields = (object[])rr[2];
            bool firsttime = true;
            StringBuilder sb = new StringBuilder();
            foreach (object[] d in directs)
            {
                if (firsttime) { sb.Append('<').Append(Dec(id)).Append('>'); firsttime = false; }
                sb.Append(" <").Append(Dec((int)d[0])).Append(">")
                    .Append(" <").Append(Dec((int)d[1])).Append("> .")
                    .Append('\n');
            }
            foreach (object[] f in fields)
            {
                if (firsttime) { sb.Append('<').Append(Dec(id)).Append('>'); firsttime = false; }
                sb.Append(" <").Append(Dec((int)f[0])).Append(">")
                    .Append(" \"").Append((string)f[1]).Append("\" .")
                    .Append('\n');
            }
            return sb.ToString();
        }
    }
}
