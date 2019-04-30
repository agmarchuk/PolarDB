using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class IndexViewImmutable : IIndexImmutable
    {
        private UniversalSequenceBase bearing;
        private UniversalSequenceBase offset_sequ;
        private Comparer<object> comp_default;
        private Func<Stream> streamGen;
        private string tmpdir;
        public Func<object, bool> Filter { get; set; }
        // создаем объект, подсоединяемся к носителям или создаем носители
        public IndexViewImmutable(Func<Stream> streamGen, UniversalSequenceBase bearing, Comparer<object> comp_d, string tmpdir, long volume_of_offset_array)
        {
            this.streamGen = streamGen;
            this.bearing = bearing;
            this.comp_default = comp_d;
            this.tmpdir = tmpdir;
            offset_sequ = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());
            this.volume_of_offset_array = volume_of_offset_array;
        }

        public void Clear()
        {
            offset_sequ.Clear();
        }

        // Что нужно? Создать и использовать
        private object[] rare_elements = null; // --

        private long volume_of_offset_array = 20_000_000;

        public void Build()
        {
            // Формируем последовательность offset_sequ
            offset_sequ.Clear();
            bearing.Scan((off, obj) =>
            {
                if (Filter == null || Filter(obj)) offset_sequ.AppendElement(off);
                return true;
            });
            offset_sequ.Flush();
            // Возможно, нам понадобятся два дополнительных стрима
            FileStream tmp_stream1 = null;
            FileStream tmp_stream2 = null;
            // Определяем рекурсивный метод построения Bld(long start_ind, long number) который в итоге переупорядочивает 
            // отрезок последовательности offset_sequ так, что ссылаемые элементы становятся отсортированными.
            void Bld(long start_ind, long number) 
            {
                if (number <= volume_of_offset_array)
                {
                    long[] offsets = new long[number];
                    object[] elements = new object[number];
                    // берем в массивы
                    for (long i=0; i < number; i++)
                    {
                        long off = (long)offset_sequ.GetByIndex(start_ind + i);
                        offsets[i] = off;
                        elements[i] = bearing.GetElement(off);
                    }
                    // Сортируем
                    Array.Sort(elements, offsets, comp_default);
                    // кладем из массивов в последовательность
                    for (long i = 0; i < number; i++)
                    {
                        if (i == 0) offset_sequ.SetElement(offsets[i], offset_sequ.ElementOffset(start_ind));
                        else        offset_sequ.SetElement(offsets[i]);
                    }

                }
                else
                {
                    // надо разбить отрезок на два, в каждом сделать сортировку, а результаты слить.
                    long firsthalf_start = start_ind;
                    long firsthalf_number = number / 2;
                    long secondhalf_start = start_ind + firsthalf_number;
                    long secondhalf_number = number - firsthalf_number;

                    Bld(firsthalf_start, firsthalf_number);
                    Bld(secondhalf_start, secondhalf_number);

                    if (tmp_stream1 == null) tmp_stream1 = File.Open(tmpdir + "tmp1.$$$", FileMode.OpenOrCreate, FileAccess.ReadWrite);
                    if (tmp_stream2 == null) tmp_stream2 = File.Open(tmpdir + "tmp2.$$$", FileMode.OpenOrCreate, FileAccess.ReadWrite);
                    tmp_stream1.Position = 0L;
                    tmp_stream2.Position = 0L;

                    byte[] buffer = new byte[1024 * 1024 * 64];

                    Stream source1 = offset_sequ.Media;
                    source1.Position = 8 + firsthalf_start * 8;
                    long nbytes1 = firsthalf_number * 8;
                    while (nbytes1 > 0)
                    {
                        int nb = source1.Read(buffer, 0, nbytes1 >= buffer.Length ? buffer.Length : (int)nbytes1);
                        tmp_stream1.Write(buffer, 0, nb); 
                        nbytes1 -= nb;
                    }
                    Stream source2 = offset_sequ.Media;
                    source2.Position = 8 + secondhalf_start * 8;
                    long nbytes2 = secondhalf_number * 8;
                    while (nbytes2 > 0)
                    {
                        int nb = source2.Read(buffer, 0, nbytes2 >= buffer.Length ? buffer.Length : (int)nbytes2);
                        tmp_stream2.Write(buffer, 0, nb);
                        nbytes2 -= nb;
                    }
                    tmp_stream1.Position = 0L;
                    BinaryReader br1 = new BinaryReader(tmp_stream1);
                    long off1 = br1.ReadInt64();
                    object obj1 = bearing.GetElement(off1);
                    long nom1 = 0; // номер обрабатываемого элемента
                    tmp_stream2.Position = 0L;
                    BinaryReader br2 = new BinaryReader(tmp_stream2);
                    long off2 = br2.ReadInt64();
                    object obj2 = bearing.GetElement(off2);
                    long nom2 = 0; // номер обрабатываемого элемента
                    long out_ind = start_ind;
                    while (nom1 < firsthalf_number && nom2 < secondhalf_number)
                    {
                        if (comp_default.Compare(obj1, obj2) <= 0)
                        {
                            offset_sequ.SetElement(off1, offset_sequ.ElementOffset(out_ind));
                            nom1++;
                            if (nom1 < firsthalf_number)
                            {
                                off1 = br1.ReadInt64();
                                obj1 = bearing.GetElement(off1);
                            }
                        }
                        else
                        {
                            offset_sequ.SetElement(off2, offset_sequ.ElementOffset(out_ind));
                            nom2++;
                            if (nom2 < secondhalf_number)
                            {
                                off2 = br2.ReadInt64();
                                obj2 = bearing.GetElement(off2);
                            }
                        }
                        out_ind++;
                    }
                    // Перепись остатков
                    if (nom1 < firsthalf_number)
                    {
                        for (long ii=nom1; ii<firsthalf_number; ii++)
                        {
                            if (ii != nom1) off1 = br1.ReadInt64();
                            offset_sequ.SetElement(off1, offset_sequ.ElementOffset(out_ind));
                            out_ind++;
                        }
                    }
                    else if (nom2 < secondhalf_number)
                    {
                        for (long ii = nom2; ii < secondhalf_number; ii++)
                        {
                            if (ii != nom2) off2 = br2.ReadInt64();
                            offset_sequ.SetElement(off2, offset_sequ.ElementOffset(out_ind));
                            out_ind++;
                        }
                    }
                }
            };

            // Исполним
            Bld(0L, offset_sequ.Count());

            if (tmp_stream1 != null)
            {
                tmp_stream1.Close();
                File.Delete(tmpdir + "tmp1.$$$");
            }
            if (tmp_stream2 != null)
            {
                tmp_stream2.Close();
                File.Delete(tmpdir + "tmp2.$$$");
            }
            Refresh();
        }
        // Коэффициент прореживания массива elements, подбирался экспериментально. Лучшие по скорости результаты 16-20
        private int Nfactor = 40;
        public void Refresh()
        {
            // построим прореженный массив значений
            //TODO: Похоже, прореживание делается правильно, но используется неправильно. Иногда выскакивает
            // ошибка, заключающаяся в том, что выдается меньше результатов. 
            rare_elements = 
                offset_sequ.ElementValues()
                .Cast<long>()
                .Where((off, i) => (i % Nfactor) == 0)
                .Select(off => bearing.GetElement((long)off))
                .ToArray();
        }

        // Поиск в последовательностях
        private IEnumerable<object> BinarySearchAll(long start, long number, object sample, Comparer<object> comp)
        {
            long half = number / 2;
            if (half == 0)
            {
                // Получаем офсет, по нему получаем объект элемента
                long offse = (long)offset_sequ.GetByIndex(start);
                object obje = bearing.GetElement(offse);
                int cmp = comp.Compare(obje, sample);
                if (cmp == 0) return Enumerable.Repeat<object>(obje, 1);
                else return Enumerable.Empty<object>(); // Не найден
            }

            long middle = start + half;
            long rest = number - half - 1;
            //object[] mid_pair = (object[])keyoffsets.GetByIndex(middle);
            long middle_offse = (long)offset_sequ.GetByIndex(middle);
            object middle_obje = bearing.GetElement(middle_offse);
            var middle_depth = comp.Compare(middle_obje, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                IEnumerable<object> flow = BinarySearchAll(start, half, sample, comp)
                    .Concat(Enumerable.Repeat<object>(middle_obje, 1));
                if (rest > 0) return flow.Concat(BinarySearchAll(middle + 1, rest, sample, comp));
                else return flow;
            }
            if (middle_depth < 0)
            {
                if (rest > 0) return BinarySearchAll(middle + 1, rest, sample, comp);
                else return Enumerable.Empty<object>();
            }
            else // middle_depth > 0
            {
                return BinarySearchAll(start, half, sample, comp);
            }
        }

        //// ================= Поиск по массиву elements в ОЗУ ==============
        //public IEnumerable<object> BinarySearchAll(object obj)
        //{
        //    long start = 0;
        //    long cnt = offset_sequ.Count();
        //    long numb = cnt;
        //    if (rare_elements != null)
        //    {
        //        var dia = BSDia(0, rare_elements.Length, obj);
        //        start = dia.Item1 * Nfactor;
        //        numb = dia.Item2 * Nfactor;
        //        if (start + numb > cnt) numb = cnt - start;
        //    }
        //    var res = BinarySearchAll(start, numb, obj);
        //    return res;
        //}

        // ================= Поиск по массиву elements в ОЗУ ==============
        public IEnumerable<object> SearchAll(object sample)
        { return SearchAll(sample, comp_default); }

        public IEnumerable<object> SearchAll(object sample, Comparer<object> c)
        {
            //long start = 0;
            //long cnt = offset_sequ.Count();
            //long numb = cnt;
            //if (rare_elements != null)
            //{
            //    var dia = BSDia(0, rare_elements.Length, sample, c);
            //    start = dia.Item1 * Nfactor;
            //    numb = dia.Item2 * Nfactor;
            //    if (start + numb > cnt) numb = cnt - start;
            //}
            if (rare_elements != null)
            {
                return SA(sample, c);
            }
            else return SearchAll0(sample, c);
        }
        private IEnumerable<object> SA(object sample, Comparer<object> c)
        {
            long pos;
            int ind = Array.BinarySearch(rare_elements, sample, c);
            if (ind >= 0)
            {
                // Найти ближайший левый индекс i, который либо 0 либо c.Compare(rare_elements[i-1], sample) < 0
                int i = ind;
                while (i > 0 && c.Compare(rare_elements[i - 1], sample) >= 0) i--;
                // Находим стартовую точку в последовательности отсортированных офсетов
                pos = (i==0?0:i-1) * Nfactor;

                //return Enumerable.Repeat(rare_elements[ind], 1);
            }
            //else return Enumerable.Empty<object>();
            else
            {
                ind = ~ind;
                pos = (ind==0?0: ind-1) * Nfactor;
            }
            // Сканируем
            while (pos < offset_sequ.Count())
            {
                object val = null;
                // Читаем значение или из массива или из последовательности
                if (pos % Nfactor == 0) val = rare_elements[pos / Nfactor];
                else val = bearing.GetElement((long)offset_sequ.GetByIndex(pos));
                // вычисляем отношение к образцу
                int cmp = c.Compare(val, sample);
                pos++;
                // пропускаем которые меньше, посылаем которые равны, выходим когда больше
                if (cmp < 0) continue;
                else if (cmp == 0) yield return val;
                else break;
            }
        }
        public IEnumerable<object> SearchAll0(object sample, Comparer<object> c)
        {
            long start = 0;
            long cnt = offset_sequ.Count();
            long numb = cnt;
            if (false && rare_elements != null) // Конструкция не работает.
            {
                var dia = BSDia(0, rare_elements.Length, sample, c);
                start = dia.Item1 * Nfactor;
                numb = dia.Item2 * Nfactor;
                if (start + numb > cnt) numb = cnt - start;
            }
            var res = BinarySearchAll(start, numb, sample, c);
            return res;
        }

        /// <summary>
        /// Ищет (минимальный) диапазон в массиве elements такой что первая точка <=0, а следующая за последней 
        /// точка - точно больше нуля. Поскольку есть пропущенные, первая точка может быть =0 только если она имеет
        /// индекс 0. Если первая точка > 0, то диапазон пустой. 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="number"></param>
        /// <param name="sample"></param>
        /// <returns>диапазон start, number в массиве elements</returns>
        private (int, int) BSDia(int start, int number, object sample, Comparer<object> current_comp)
        {
            if (number == 0) return (start, 0);
            if (current_comp.Compare(rare_elements[start], sample) > 0) return (start, 0);
            if (number == 1) return (start, number);

            int half = number / 2;
            int middle = start + half;
            int rest = number - half - 1;
            object middle_obje = rare_elements[middle];
            var middle_depth = current_comp.Compare(middle_obje, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                int sta, num;
                var left = BSDia(start, half, sample, current_comp);
                if (left.Item2 == 0) { sta = half; num = 1; }
                else { sta = left.Item1; num = left.Item2 + 1; }
                if (rest > 0) { var right = BSDia(middle + 1, rest, sample, current_comp); num += right.Item2; }
                return (sta, num);
            }
            if (middle_depth < 0)
            {
                if (rest == 0) return (middle, 1);
                var d = BSDia(middle + 1, rest, sample, current_comp);
                if (d.Item2 == 0) return (middle, 1);
                return d;
            }
            else // middle_depth > 0
            {
                return BSDia(start, half, sample, current_comp);
            }
        }

    }
}
