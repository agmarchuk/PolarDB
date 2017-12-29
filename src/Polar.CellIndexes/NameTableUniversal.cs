using System;
using System.Collections.Generic;
using System.Linq;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class NameTableUniversal
    {
        private string path;
        private TableView table;
        private IndexDynamic<int, IndexViewImmutable<int>> offsets;
        private IndexViewImmutable<int> offset_array;
        private IndexHalfkeyImmutable<string> s_index_array;
        private string s_index_array_path;
        private IndexDynamic<string, IndexHalfkeyImmutable<string>> s_index;
        public NameTableUniversal(string path) 
        {
            this.path = path;
            PType tp_tabelement = new PTypeRecord(
                new NamedType("code", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)));
            this.table = new TableView(path + "cstable", tp_tabelement);
            //next_code = (int)table.Count();
            offset_array = new IndexViewImmutable<int>(path + "offsets")
            {
                Table = this.table,
                KeyProducer = pair => (int)((object[])(((object[])pair)[1]))[0], 
                tosort = false
            };
            offsets = new IndexDynamic<int, IndexViewImmutable<int>>(true)
            {
                Table = this.table,
                //KeyProducer = pair => (int)((object[])pair)[0],
                KeyProducer = pair => (int)((object[])(((object[])pair)[1]))[0],
                IndexArray = offset_array
            };
            table.RegisterIndex(offsets);
            s_index_array_path = path + "s_index";

            s_index_array = new IndexHalfkeyImmutable<string>(s_index_array_path)
            {
                Table = table,
                KeyProducer = pair => (string)((object[])(((object[])pair)[1]))[1],
                HalfProducer = key => key.GetHashCode()
            };
            s_index_array.Scale = new ScaleCell(path + "dyna_index_str_half") { IndexCell = s_index_array.IndexCell };
            //s_index_array.Scale = new ScaleMemory() { IndexCell = s_index_array.IndexCell };
            s_index = new IndexDynamic<string, IndexHalfkeyImmutable<string>>(true)
            {
                Table = table,
                KeyProducer = pair => (string)((object[])(((object[])pair)[1]))[1],
                IndexArray = s_index_array
            };
            table.RegisterIndex(s_index);
        }
        public void Warmup() { table.Warmup(); offset_array.Warmup(); s_index_array.Warmup(); }
        //public void ActivateCache()
        //{
        //    table.ActivateCache();
        //    offset_array.ActivateCache();
        //    s_index_array.ActivateCache();
        //}
        public void Clear() 
        { 
            table.Clear(); 
        }
        public void Fill(IEnumerable<string> different_strings) 
        {
            table.Fill(different_strings.Select((s, i) => new object[] { i, s }));
        }
        public void BuildIndexes()
        {
            offsets.Build();
            offset_array.Build(); //TODO: Здесь надо отменить сортировку
            s_index.Build();
            BuildScale();
        }
        public void BuildScale()
        {
            if (s_index_array.Scale != null) s_index_array.Scale.Build();
        }
        // Проверяет и, если надо, добавляет. Выдает код.
        //private int next_code;
        public int Add(string s)
        {
            var q = s_index.GetAllByKey(s)
                //.Where() // проверить на deleted?
                .Select(ent => ent.Get())
                .FirstOrDefault();
            int code;
            if (q == null)
            {
                //code = next_code;
                //table.AppendValue(new object[] { next_code, s });
                //next_code++;
                code = (int)table.Count();
                table.AppendValue(new object[] { code, s });
            }
            else
            {
                code = (int)((object[])((object[])q)[1])[0];
            }
            return code;
        }
        public int GetCodeByString(string s)
        {
            var q = s_index.GetAllByKey(s)
                //.Where() // проверить на deleted?
                .Select(ent => ent.Get())
                .FirstOrDefault();
            if (q == null) return -1;
            return (int)((object[])((object[])q)[1])[0];
        }
        public string GetStringByCode(int cod)
        {
            // Старый вариант, Но другого нет...
            var qu = offsets.GetAllByKey(cod)
                //.Where() // какие-то проверки?
                .Select(ent => ent.Get())
                .FirstOrDefault();

            if (qu == null) return null;
            return (string)((object[])((object[])qu)[1])[1];
        }

        public class HashedString : IComparable
        {
            private string s;
            private int h;
            public string Str { get { return s; } set { s = value; h = s.GetHashCode(); } }
            public int Hash { get { return h; } }
            public int CompareTo(object ob)
            {
                HashedString hs = (HashedString)ob;
                int cmp = h.CompareTo(hs.Hash);
                if (cmp == 0) cmp = s.CompareTo(hs.Str);
                return cmp;
            }
        }
        // =========== Ключевой фрагмент ============
        public Dictionary<string, int> InsertPortion(IEnumerable<string> s_flow)
        {
            HashSet<string> hs = new HashSet<string>();
            foreach (string s in s_flow) hs.Add(s);

            //string[] ssa = hs.OrderBy(s => new HashedString() { Str = s }).ToArray(); // Надо сделать более экономно
            string[] ssa = hs.Select(s => new { s = s, hs = new HashedString() { Str = s } })
                .OrderBy(pa => pa.hs)
                .Select(pa => pa.s).ToArray();
                
            if (ssa.Length == 0) return new Dictionary<string, int>();

            //s_index_array.IndexCell.Close(); // cssequence.Close();
            // Подготовим основную ячейку для работы
            if (System.IO.File.Exists(path + "tmp.pac")) System.IO.File.Delete(path + "tmp.pac");
            //System.IO.File.Copy(s_index_array_path + ".pac", path + "tmp.pac");

            // Это по общей логике, но если снаружи изменится, надо изменить и тут
            PType tp_s_index_seq = new PTypeSequence(new PTypeRecord(
                new NamedType("halfkey", new PType(PTypeEnumeration.integer)),
                new NamedType("offset", new PType(PTypeEnumeration.longinteger))));
            PaCell source = new PaCell(tp_s_index_seq, path + "tmp.pac", false);
            source.Fill(new object[0]);
            foreach (var v in s_index_array.IndexCell.Root.ElementValues()) source.Root.AppendElement(v);
            source.Flush();
            PaCell target = s_index_array.IndexCell;
            target.Clear();
            target.Fill(new object[0]);

            int ssa_ind = 0;
            bool ssa_notempty = true;
            string ssa_current = ssa_notempty ? ssa[ssa_ind] : null;
            ssa_ind++;

            // Для накопления пар  
            List<KeyValuePair<string, int>> accumulator = new List<KeyValuePair<string, int>>(ssa.Length);

            // Очередной (новый) код (индекс)
            int code_new = 0;
            if (!source.IsEmpty && source.Root.Count() > 0)
            {
                code_new = (int)source.Root.Count();
                PaEntry tab_entry = table.Element(0); // не было проверки на наличие хотя бы одного элемента
                // Сканируем индексный массив, элементы являются парами {halfkey, offset}
                foreach (object[] val in source.Root.ElementValues())
                {
                    // Пропускаю элементы из нового потока, которые меньше текущего сканированного элемента 
                    int halfkey = (int)val[0];
                    string s = null; // Будет запрос если понадобится
                    int cmp = 0;
                    while (ssa_notempty) //  && (cmp = ssa_current.CompareTo(s)) <= 0
                    {
                        int hash_current = ssa_current.GetHashCode();
                        cmp = hash_current.CompareTo(halfkey);
                        if (cmp == 0)
                        { // Дополнительное упрядочивание по строке
                            if (s == null)
                            {
                                tab_entry.offset = (long)val[1];
                                s = (string)tab_entry.Field(1).Field(1).Get();
                            }
                            cmp = ssa_current.CompareTo(s);
                        }
                        if (cmp < 0)
                        { // добавляется новый код
                            // добавляем код в таблицу
                            long offset = table.TableCell.Root.AppendElement(new object[] { false, new object[] { code_new, ssa_current } });
                            // Автоматом добавляем начало строки в offsets
                            offset_array.IndexCell.Root.AppendElement(offset);
                            // добавляем строчку в строковый индекс
                            target.Root.AppendElement(new object[] { hash_current, offset });
                            accumulator.Add(new KeyValuePair<string, int>(ssa_current, code_new));
                            code_new++;
                        }
                        else if (cmp == 0)
                        { // используется существующий код
                            tab_entry.offset = (long)val[1];
                            object[] ob = (object[])tab_entry.Get();
                            object[] rec = (object[])ob[1];
                            int code = (int)rec[0];
                            string key = (string)rec[1];
                            accumulator.Add(new KeyValuePair<string, int>(key, code));
                        }
                        else // if (cmp > 0)
                            break; // Нужно дойти до него на следующем элементе в следующем цикле
                        if (ssa_ind < ssa.Length)
                            ssa_current = ssa[ssa_ind++]; //ssa.ElementAt<string>(ssa_ind);
                        else
                            ssa_notempty = false;
                    }
                    target.Root.AppendElement(val); // переписывается тот же объект
                }
            }
            // В массиве ssa могут остаться элементы, их надо просто добавить
            if (ssa_notempty)
            {
                do
                {
                    // добавляем код в таблицу
                    long offset = table.TableCell.Root.AppendElement(new object[] { false, new object[] { code_new, ssa_current } });
                    // Автоматом добавляем начало строки в offsets
                    offset_array.IndexCell.Root.AppendElement(offset);
                    // добавляем строчку в строковый индекс
                    target.Root.AppendElement(new object[] { ssa_current.GetHashCode(), offset });
                    accumulator.Add(new KeyValuePair<string, int>(ssa_current, code_new));
                    code_new++;
                    if (ssa_ind < ssa.Length) ssa_current = ssa[ssa_ind];
                    ssa_ind++;
                }
                while (ssa_ind <= ssa.Length);
            }

            table.TableCell.Flush();
            offset_array.IndexCell.Flush();
            target.Flush();
            
            source.Close();
            System.IO.File.Delete(path + "tmp.pac");

            // Финальный аккорд: формирование и выдача словаря
            Dictionary<string, int> dic = new Dictionary<string, int>();
            foreach (var keyValuePair in accumulator.Where(keyValuePair => !dic.ContainsKey(keyValuePair.Key)))
            {
                dic.Add(keyValuePair.Key, keyValuePair.Value);
            }

            return dic;
        }


        public override string ToString()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            sb.Append('[');
            table.Scan((off, obj) =>
            {
                object ob = ((object[])obj)[1];
                sb.Append('{');
                sb.Append(((object[])ob)[0]);
                sb.Append(' ');
                sb.Append(((object[])ob)[1]);
                sb.Append('}');
                return true;
            });
            sb.Append(']');
            sb.Append(offset_array.ToString());
            sb.Append(s_index.ToString());
            return sb.ToString();
        }
        public static void Main8()
        {
            string path = "../../../Databases/";
            Console.WriteLine("Start NameTableUniversal.");
            NameTableUniversal ntu = new NameTableUniversal(path);
            int test = 4;

            if (test == 1)
            {
                string[] strs = { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine" };
                ntu.Fill(strs);
                ntu.BuildIndexes();
                ntu.Add("ten");
                Console.WriteLine(ntu.ToString());
            }

            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            System.Random rnd = new Random();

            if (test == 2)
            {
                // Тест на ввод данных
                var different_set_query = Enumerable.Range(0, 1000000).Select(i => i.ToString());
                sw.Restart();
                ntu.Fill(different_set_query);
                sw.Stop();
                System.Console.WriteLine("Load ok. Duration={0}", sw.ElapsedMilliseconds);
                sw.Restart();
                ntu.BuildIndexes();
                sw.Stop();
                System.Console.WriteLine("Build indexes ok. Duration={0}", sw.ElapsedMilliseconds);
                sw.Restart();
                for (int i = 0; i < 1000; i++)
                {
                    int code = ntu.GetCodeByString(rnd.Next(1500000).ToString());
                    //Console.WriteLine(i);
                }
                sw.Stop();
                System.Console.WriteLine("1000 GetCodeByIndex ok. Duration={0}", sw.ElapsedMilliseconds);
            }

            if (test == 3)
            {
                string[] strs1 = { "zero", "one", "two", "three", "four", "five" };
                string[] strs2 = { "six", "seven", "eight", "nine" };
                //var query = Enumerable.Repeat<int>(0, 100000).Select(x => rnd.Next());
                sw.Restart();
                ntu.Fill(new string[0]);
                ntu.BuildIndexes();
                ntu.InsertPortion(strs1);
                ntu.InsertPortion(strs2);
                ntu.BuildScale();
                sw.Stop();
                Console.WriteLine(ntu.ToString());
                System.Console.WriteLine(" ok. Duration={0}", sw.ElapsedMilliseconds);
            }

            if (test == 4)
            {
                int portion = 500000;
                int nportions = 2;
                int limit = 1000000000;
                var query = Enumerable.Repeat<int>(0, portion).Select(x => rnd.Next(limit).ToString());
                bool tobuild = true;
                if (tobuild)
                {
                    sw.Restart();
                    ntu.Fill(new string[0]);
                    ntu.BuildIndexes();
                    for (int i = 0; i < nportions; i++)
                    {
                        ntu.InsertPortion(query);
                        Console.WriteLine("portion {0} ok.", i + 1);
                    }
                    ntu.BuildScale();
                    sw.Stop();
                    System.Console.WriteLine("Load-build ok. Duration={0}", sw.ElapsedMilliseconds);
                }
                else
                {
                    sw.Restart();
                    ntu.Warmup();
                    sw.Stop();
                    System.Console.WriteLine("Warmup ok. Duration={0} volume={1}", sw.ElapsedMilliseconds, ntu.table.TableCell.Root.Count());
                }
                sw.Restart();
                int number = 0;
                foreach (var s in query.Take(10000))
                {
                    int c = ntu.GetCodeByString(s);
                    number += c < 0 ? 0 : 1;
                }
                sw.Stop();
                System.Console.WriteLine("10000 search ok. Duration={0} number={1}", sw.ElapsedMilliseconds, number);
            }

            // Выполним только после уже построенной таблицы имен. От нее должна остаться опорная таблица cstable.pac
            if (test == 5)
            {
                int portion = 500000;
                //int nportions = 2;
                int limit = 1000000000;
                var query = Enumerable.Repeat<int>(0, portion).Select(x => rnd.Next(limit).ToString());
                bool tobuild = true;
                if (tobuild)
                {
                    sw.Restart();
                    ntu.BuildIndexes();
                    sw.Stop();
                    System.Console.WriteLine("BuildIndexes ok. Duration={0}", sw.ElapsedMilliseconds);
                }
                else
                {
                    sw.Restart();
                    ntu.Warmup();
                    sw.Stop();
                    System.Console.WriteLine("Warmup ok. Duration={0} volume={1}", sw.ElapsedMilliseconds, ntu.table.TableCell.Root.Count());
                }
                sw.Restart();
                int number = 0;
                foreach (var s in query.Take(10000))
                {
                    int c = ntu.GetCodeByString(s);
                    number += c < 0 ? 0 : 1;
                }
                sw.Stop();
                System.Console.WriteLine("10000 search ok. Duration={0} number={1}", sw.ElapsedMilliseconds, number);

            }

        }
    }
}
