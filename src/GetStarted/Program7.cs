using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polar.DB;
using Polar.Cells;
using Polar.CellIndexes;
using Polar.PagedStreams;

namespace GetStarted
{
    public partial class Program
    {
        //static string path = "Databases/";
        public static void Main7()
        {
            Random rnd = new Random(777);
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start demo key-value storage");
            // Тип основной таблицы
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));

            // ======================== Теперь нам понадобится страничное хранилище =========================
            // файл - носитель хранилища
            string dbpath = path + "storage7.bin";
            bool fob_exists = File.Exists(dbpath);
            FileStream fs = new FileStream(dbpath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            FileOfBlocks fob = new FileOfBlocks(fs);
            Stream first_stream = fob.GetFirstAsStream();
            if (!fob_exists)
            {
                PagedStream.InitPagedStreamHead(first_stream, 8L, 0, PagedStream.HEAD_SIZE);
                fob.Flush();
            }
            PagedStream main_stream = new PagedStream(fob, fob.GetFirstAsStream(), 8L);

            long sz = PagedStream.HEAD_SIZE;
            // === В main_stream первым объектом поместим тествый поток
            // Если main_stream нулевой длины, надо инициировать конфигурацию стримов
            if (main_stream.Length == 0)
            {
                // инициируем 3 головы для потоков
                PagedStream.InitPagedStreamHead(main_stream, 0L, 0L, -1L);
                PagedStream.InitPagedStreamHead(main_stream, sz, 0L, -1L);
                PagedStream.InitPagedStreamHead(main_stream, 2 * sz, 0L, -1L);
                main_stream.Flush(); fob.Flush();
            }
            // создадим 3 потока
            PagedStream stream_person = new PagedStream(fob, main_stream, 0L);
            PagedStream stream_keys = new PagedStream(fob, main_stream, 1 * sz);
            PagedStream stream_offsets = new PagedStream(fob, main_stream, 2 * sz);



            // =========================== OK ==========================

            // Создадим базу данных, состоящую из последовательности, и двух индексных массивов: массива ключей и массива офсетов 
            PaCell person_seq = new PaCell(new PTypeSequence(tp_person), stream_person, false);
            Func<object, int> person_code_keyproducer = v => (int)((object[])v)[0];
            PaCell pers_ind_key_arr = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.integer)), stream_keys, false);
            PaCell pers_ind_off_arr = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.longinteger)), stream_offsets, false);

            int nelements = 1000000;
            bool toload = true; // Загружать или нет новую базу данных
            if (toload)
            {
                sw.Restart();
                // При загрузке кеш будет мешаться, его полезно деактивировать
                fob.DeactivateCache();
                // Очистим ячейку последовательности 
                person_seq.Clear();
                person_seq.Fill(new object[0]);

                // Для загрузки нам понадобятся: поток данных, массив ключей и массив офсетов (это всего лишь демонстрация!)
                IEnumerable<object> flow = Enumerable.Range(0, nelements)
                    .Select(iv =>
                    {
                        int id = nelements - iv;
                        string name = "=" + id.ToString() + "=";
                        double age = rnd.NextDouble() * 100.0;
                        return new object[] { id, name, age };
                    });
                int[] key_arr = new int[nelements];
                long[] off_arr = new long[nelements];

                int ind = 0;
                foreach (object[] orec in flow)
                {
                    long off = person_seq.Root.AppendElement(orec);
                    int key = (int)(((object[])orec)[0]);
                    key_arr[ind] = key;
                    off_arr[ind] = off;
                    ind++;
                }
                person_seq.Flush();
                Console.WriteLine("Sequence OK. duration={0}", sw.ElapsedMilliseconds);
                // Параллельная сортировка по значению ключа
                Array.Sort(key_arr, off_arr);

                pers_ind_key_arr.Clear();
                pers_ind_key_arr.Fill(new object[0]);
                for (int i = 0; i < nelements; i++) pers_ind_key_arr.Root.AppendElement(key_arr[i]);
                pers_ind_key_arr.Flush();

                pers_ind_off_arr.Clear();
                pers_ind_off_arr.Fill(new object[0]);
                for (int i = 0; i < nelements; i++) pers_ind_off_arr.Root.AppendElement(off_arr[i]);
                pers_ind_off_arr.Flush();

                fob.Flush();

                sw.Stop();
                Console.WriteLine("Load ok. duration for {0} elements: {1} ms", nelements, sw.ElapsedMilliseconds);
            }

            sw.Restart();
            fob.LoadCache();
            sw.Stop();
            Console.WriteLine("Cache load duration={0}", sw.ElapsedMilliseconds);

            sw.Restart();
            int nkeys = (int)pers_ind_key_arr.Root.Count();
            //int[] keys = new int[nkeys];
            //int ii = 0;
            //foreach (int k in pers_ind_key_arr.Root.ElementValues()) { keys[ii] = k; ii++; }
            int[] keys = pers_ind_key_arr.Root.ElementValues().Cast<int>().ToArray();
            long[] offsets = pers_ind_off_arr.Root.ElementValues().Cast<long>().ToArray();

            sw.Stop();
            Console.WriteLine("Index preload duration={0}", sw.ElapsedMilliseconds);

            var GetDia = GetDiaFunc3(keys);

            PaEntry entry = person_seq.Root.Element(0);
            int s_key = nelements * 2 / 3;
            int iifirst = Get7FirstIndexOf(0, nelements, keys, s_key);
            if (iifirst == -1) throw new Exception($"Cant find {s_key}");
            Diapason dia = GetDia(s_key);
            int iisecond = Get7FirstIndexOf((int)dia.start, (int)dia.numb, keys, s_key);
            //long offs = (long)pers_ind_off_arr.Root.Element(iifirst).Get();
            long offs = offsets[iifirst];
            if (iisecond != iifirst) throw new Exception($"Err in diapazon locating");
            entry.offset = offs;
            object[] re = (object[])entry.Get();
            Console.WriteLine($"{re[0]} {re[1]} {re[2]}");

            int nte = 100000;
            int[] testkeys = Enumerable.Repeat<int>(0, nte).Select(i => rnd.Next(nkeys) + 1).ToArray();
            sw.Restart();
            for (int i = 0; i < nte; i++)
            {
                //int key = rnd.Next(nkeys) + 1;
                int search_key = testkeys[i];
                //int ifirst = Get7FirstIndexOf(0, nelements, keys, key);
                Diapason diap = GetDia(search_key);
                int ifirst = Get7FirstIndexOf((int)diap.start, (int)diap.numb, keys, search_key);
                if (ifirst == -1) { Console.WriteLine($"Cant find key={search_key}"); continue; }
                long offset = offsets[ifirst];
                entry.offset = offset;
                object[] rec = (object[])entry.Get();
            }
            sw.Stop();
            Console.WriteLine("Duration={0} for {1}", sw.ElapsedMilliseconds, nte);

        }
        internal static Func<int, Diapason> GetDiaFunc3(int[] keys)
        {
            // Построение шкалы
            int N = keys.Length;
            int min = keys[0];
            int max = keys[N - 1];
            int n_scale = N / 16; // + (N % 16 != 0 ? 1 : 0);
            int[] starts;
            Func<int, int> ToPosition;

            // Особый случай, когда n_scale < 1 или V_min == V_max. Тогда делается массив из одного элемента и особая функция
            if (n_scale < 1 || min == max)
            {
                n_scale = 1;
                starts = new int[1];
                starts[0] = 0;
                ToPosition = (int key) => 0;
            }
            else
            {
                starts = new int[n_scale];
                ToPosition = (int key) => (int)(((long)key - (long)min) * (long)(n_scale - 1) / (long)((long)max - (long)min));
            }
            // Заполнение количеств элементов в диапазонах
            for (int i = 0; i < keys.Length; i++)
            {
                int key = keys[i];
                int position = ToPosition(key);
                // Предполагаю, что начальная разметка массива - нули
                starts[position] += 1;
            }
            // Заполнение начал диапазонов
            int sum = 0;
            for (int i = 0; i < n_scale; i++)
            {
                int num_els = starts[i];
                starts[i] = sum;
                sum += num_els;
            }

            Func<int, Diapason> GetDia = key =>
            {
                if (key > max - 16)
                {

                }
                int ind = ToPosition(key);
                if (ind < 0 || ind >= n_scale)
                {
                    return Diapason.Empty;
                }
                else
                {
                    int sta = starts[ind];
                    int num = ind < n_scale - 1 ? starts[ind + 1] - sta : keys.Length - sta;
                    return new Diapason() { start = sta, numb = num };
                }
            };
            return GetDia;
        }

        private static int Get7FirstIndexOf(int start, int number, int[] keys, int key)
        {
            int half = number / 2;
            if (number < 1)
            {
                return -1;
            } // Не найден
            int middle = start + half;
            var middle_depth = keys[middle].CompareTo(key);
            if (middle_depth == 0) return middle;
            if (middle_depth < 0)
            {
                return Get7FirstIndexOf(middle + 1, number - half - 1, keys, key);
            }
            else
            {
                return Get7FirstIndexOf(start, half, keys, key);
            }
        }
    }
}
