﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class IndexViewImm
    {
        private UniversalSequenceBase bearing;
        private UniversalSequenceBase offset_sequ;
        private Comparer<object> comp;
        private Func<Stream> streamGen;
        private string tmpdir;
        // создаем объект, подсоединяемся к носителям или создаем носители
        public IndexViewImm(Func<Stream> streamGen, UniversalSequenceBase bearing, Comparer<object> comp, string tmpdir)
        {
            this.streamGen = streamGen;
            this.bearing = bearing;
            this.comp = comp;
            this.tmpdir = tmpdir;
            offset_sequ = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), streamGen());
        }
        // Что нужно? Создать и использовать
        private object[] rare_elements = null; // --

        private long volume_of_offset_array = 500_000;

        public void Build()
        {
            // Формируем последовательность offset_sequ
            offset_sequ.Clear();
            bearing.Scan((off, obj) =>
            {
                offset_sequ.AppendElement(off);
                return true;
            });
            offset_sequ.Flush();
            // Возможно, нам понадобятся два дополнительных стрима
            FileStream tmp_stream1 = null;
            FileStream tmp_stream2 = null;
            // Определяем рекурсивный метод построения Bld(long start_ind, long number) который в итоге переупорядочивает 
            // отрезок последовательности offset_sequ так, что ссылаемые элементы становятся отсортированными.
            Action<long, long> Bld = null;
            Bld = (start_ind, number) =>
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
                    Array.Sort(elements, offsets, comp);
                    // кладем из массивов в последовательность
                    for (long i = 0; i < number; i++)
                    {
                        if (i == 0) offset_sequ.SetElement(offsets[i], start_ind);
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
                    while (nom1 <= firsthalf_number && nom2 <= secondhalf_number)
                    {
                        if (comp.Compare(obj1, obj2) <= 0)
                        {
                            offset_sequ.SetElement(off1, offset_sequ.ElementOffset(out_ind));
                            off1 = br1.ReadInt64();
                            obj1 = bearing.GetElement(off1);
                            nom1++;
                        }
                        else
                        {
                            offset_sequ.SetElement(off2, offset_sequ.ElementOffset(out_ind));
                            off2 = br2.ReadInt64();
                            obj2 = bearing.GetElement(off2);
                            nom2++;
                        }
                        out_ind++;
                    }
                    // Перепись остатков
                    if (nom1 < firsthalf_number)
                    {
                        for (long ii=nom1; ii<firsthalf_number; ii++)
                        {
                            if (ii != 0) off1 = br1.ReadInt64();
                            offset_sequ.SetElement(off1, offset_sequ.ElementOffset(out_ind));
                            out_ind++;
                        }
                    }
                    else if (nom2 < secondhalf_number)
                    {
                        for (long ii = nom1; ii < firsthalf_number; ii++)
                        {
                            if (ii != 0) off1 = br1.ReadInt64();
                            offset_sequ.SetElement(off1, offset_sequ.ElementOffset(out_ind));
                            out_ind++;
                        }
                    }
                }
            };
            // Исполним
            Bld(0L, bearing.Count());
            // построим прореженный массив
            rare_elements = offset_sequ.ElementValues().Where((off, i) => (i % Nfactor == 0))
                .Select(off => bearing.GetElement((long)off)).ToArray();
        }
        public void Build0()
        {
            long[] offsets; // временное решение
            object[] elements; // --
            int ne = (int)bearing.Count();
            // формируем два массива
            offsets = new long[ne];
            elements = new object[ne];
            int ind = 0;
            bearing.Scan((off, obj) => 
            {
                offsets[ind] = off;
                elements[ind] = obj;
                ind++;
                return true;
            });
            // Сортируем
            Array.Sort(elements, offsets, comp);

            // очищаем индексный массив
            offset_sequ.Clear();
            for (int i=0; i<ne; i++)
            {
                offset_sequ.AppendElement(offsets[i]);
            }
            offset_sequ.Flush();

            // Теперь оставим только часть массивов
            rare_elements = elements.Where((obj, i) => i % Nfactor == 0).ToArray();
            //offsets = offsets.Where((o, i) => i % Nfactor == 0).ToArray();

        }
        // Коэффициент прореживания массива elements, подбирался экспериментально. Лучшие по скорости результаты 16-20
        private int Nfactor = 40;
        public void Refresh() { }

        // Поиск в последовательностях
        private IEnumerable<object> BinarySearchAll(long start, long number, object sample)
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
            //var middle_depth = comp.Compare(mid_pair, key, sample);
            var middle_depth = comp.Compare(middle_obje, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                IEnumerable<object> flow = BinarySearchAll(start, half, sample)
                    .Concat(Enumerable.Repeat<object>(middle_obje, 1));
                if (rest > 0) return flow.Concat(BinarySearchAll(middle + 1, rest, sample));
                else return flow;
            }
            if (middle_depth < 0)
            {
                if (rest > 0) return BinarySearchAll(middle + 1, rest, sample);
                else return Enumerable.Empty<object>();
            }
            else // middle_depth > 0
            {
                return BinarySearchAll(start, half, sample);
            }
        }

        // ================= Поиск по массиву elements в ОЗУ ==============
        public IEnumerable<object> BinarySearchAll(object obj)
        {
            long start = 0;
            long numb = offset_sequ.Count();
            if (rare_elements != null)
            {
                var dia = BSDia(0, rare_elements.Length, obj);
                start = dia.Item1 * Nfactor;
                numb = dia.Item2 * Nfactor;
            }
            var res = BinarySearchAll(start, numb, obj);
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
        private (int, int) BSDia(int start, int number, object sample)
        {
            if (number == 0) return (start, 0);
            if (comp.Compare(rare_elements[start], sample) > 0) return (start, 0);
            if (number == 1) return (start, number);

            int half = number / 2;
            int middle = start + half;
            int rest = number - half - 1;
            object middle_obje = rare_elements[middle];
            var middle_depth = comp.Compare(middle_obje, sample);

            if (middle_depth == 0)
            { // Вариант {левый, центральная точка, возможно правый}
                int sta, num;
                var left = BSDia(start, half, sample);
                if (left.Item2 == 0) { sta = half; num = 1; }
                else { sta = left.Item1; num = left.Item2 + 1; }
                if (rest > 0) { var right = BSDia(middle + 1, rest, sample); num += right.Item2; }
                return (sta, num);
            }
            if (middle_depth < 0)
            {
                if (rest == 0) return (middle, 1);
                var d = BSDia(middle + 1, rest, sample);
                if (d.Item2 == 0) return (middle, 1);
                return d;
            }
            else // middle_depth > 0
            {
                return BSDia(start, half, sample);
            }
        }

    }
}