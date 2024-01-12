using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.DB
{
    public class Scale
    {
        private int keysLength, n_scale, min, max;
        public Func<int, Diapason> GetDia = null;
        private int[] starts = null;
        private Func<int, int> ToPosition = null;
        private UniversalSequenceBase keylengthminmaxstarts;

        public Scale(Stream stream)
        {
            keylengthminmaxstarts = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream);
            int nvalues = (int)keylengthminmaxstarts.Count();
            if (nvalues > 0)
            {
                keysLength = (int)keylengthminmaxstarts.GetByIndex(0);
                min = (int)keylengthminmaxstarts.GetByIndex(1);
                max = (int)keylengthminmaxstarts.GetByIndex(2);
                n_scale = nvalues - 3;
                starts = new int[n_scale];
                for (int i = 3; i < nvalues; i++) starts[i - 3] = (int)keylengthminmaxstarts.GetByIndex(i);
                SetToPosition();
                SetGetDia();
            }
        }
        public void Close()
        {
            keylengthminmaxstarts.Close();
        }
        public void Load(int[] keys)
        {
            keysLength = keys.Length;
            if (keysLength == 0) return;
            n_scale = keysLength / 16;
            min = keys[0];
            max = keys[keysLength - 1];

            // Особый случай, когда n_scale < 1 или V_min == V_max. Тогда делается массив из одного элемента и особая функция
            if (n_scale < 1 || min == max)
            {
                n_scale = 1;
                starts = new int[1];
                starts[0] = 0;
            }
            else
            {
                starts = new int[n_scale];
            }
            SetToPosition();
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
            SetGetDia();
            // Запись наработанного в стрим
            keylengthminmaxstarts.Clear();
            keylengthminmaxstarts.AppendElement(keysLength);
            keylengthminmaxstarts.AppendElement(min);
            keylengthminmaxstarts.AppendElement(max);
            for (int i=0; i<starts.Length; i++)
            {
                keylengthminmaxstarts.AppendElement(starts[i]);
            }
            keylengthminmaxstarts.Flush();
        }

        private void SetToPosition()
        {
            if (starts.Length == 1)
                ToPosition = (int key) => 0;
            else
                ToPosition = (int key) => (int)(((long)key - (long)min) * (long)(n_scale - 1) / (long)((long)max - (long)min));
        }
        private void SetGetDia()
        {
            GetDia = key =>
            {

                if (key > max - 16)
                {
                    // ???
                }
                int ind = ToPosition(key);
                if (ind < 0 || ind >= n_scale)
                {
                    return Diapason.Empty;
                }
                else
                {
                    int sta = starts[ind];
                    int num = ind < n_scale - 1 ? starts[ind + 1] - sta : keysLength - sta;
                    return new Diapason() { start = sta, numb = num };
                }
            };
        }





    // ============ Работа со шкалой - базовый вариант ==============
    /// <summary>
    /// Формирование шкалы в виде функции, вычисляющей по образцу ключа диапазон в последовательности 
    /// где он может находиться. Есть рекомендованные значения параметров.
    /// </summary>
    /// <param name="keys"></param>
    /// <param name="min">min = keys[0]</param>
    /// <param name="max">max = keys[N - 1]</param>
    /// <param name="n_scale">n_scale = N / 16</param>
    /// <returns></returns>
    public static Func<int, Diapason> GetDiaFunc32(IEnumerable<int> keys, int min, int max, int n_scale)
        {
            //if (keys == null || keys.Length == 0) return null;
            //// Построение шкалы
            //int N = keys.Length;
            //int min = keys[0];
            //int max = keys[N - 1];
            //int n_scale = N / 16; // + (N % 16 != 0 ? 1 : 0);

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
            //for (int i = 0; i < keys.Length; i++)
            //{
            //    int key = keys[i];
            //    int position = ToPosition(key);
            //    // Предполагаю, что начальная разметка массива - нули
            //    starts[position] += 1;
            //}
            int keysLength = 0;
            foreach (var key in keys)
            {
                int position = ToPosition(key);
                // Предполагаю, что начальная разметка массива - нули
                starts[position] += 1;
                keysLength++;
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
                { //???
                }
                int ind = ToPosition(key);
                if (ind < 0 || ind >= n_scale)
                {
                    return Diapason.Empty;
                }
                else
                {
                    int sta = starts[ind];
                    int num = ind < n_scale - 1 ? starts[ind + 1] - sta : keysLength - sta;
                    return new Diapason() { start = sta, numb = num };
                }
            };
            return GetDia;
        }
        // ============ Работа со шкалой ==============
        public static Func<int, Diapason> GetDiaFunc32(int[] keys)
        {
            if (keys == null || keys.Length == 0) return null;
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
        public static Func<long, Diapason> GetDiaFunc64(long[] keys)
        {
            if (keys == null || keys.Length == 0) return null;
            // Построение шкалы
            int N = keys.Length;
            long min = keys[0];
            long max = keys[N - 1];
            int n_scale = N / 16; // + (N % 16 != 0 ? 1 : 0);
            int[] starts;
            Func<long, int> ToPosition;

            // Особый случай, когда n_scale < 1 или V_min == V_max. Тогда делается массив из одного элемента и особая функция
            if (n_scale < 1 || min == max)
            {
                n_scale = 1;
                starts = new int[1];
                starts[0] = 0;
                ToPosition = (long key) => 0;
            }
            else
            {
                starts = new int[n_scale];
                ToPosition = (long key) => (int)(((long)key - (long)min) * (long)(n_scale - 1) / (long)((long)max - (long)min));
            }
            // Заполнение количеств элементов в диапазонах
            for (int i = 0; i < keys.Length; i++)
            {
                long key = keys[i];
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

            Func<long, Diapason> GetDia = key =>
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
        //public static Func<object, Diapason> GetDiaFunc(object[] keys)
        //{
        //    if (keys == null || keys.Length == 0) return null;
        //    // Построение шкалы
        //    int N = keys.Length;
        //    int min = (int)keys[0];
        //    int max = (int)keys[N - 1];
        //    int n_scale = N / 16; // + (N % 16 != 0 ? 1 : 0);
        //    int[] starts;
        //    Func<int, int> ToPosition;

        //    // Особый случай, когда n_scale < 1 или V_min == V_max. Тогда делается массив из одного элемента и особая функция
        //    if (n_scale < 1 || min == max)
        //    {
        //        n_scale = 1;
        //        starts = new int[1];
        //        starts[0] = 0;
        //        ToPosition = (int key) => 0;
        //    }
        //    else
        //    {
        //        starts = new int[n_scale];
        //        ToPosition = (int key) => (int)(((long)key - (long)min) * (long)(n_scale - 1) / (long)((long)max - (long)min));
        //    }
        //    // Заполнение количеств элементов в диапазонах
        //    for (int i = 0; i < keys.Length; i++)
        //    {
        //        int key = (int)keys[i];
        //        int position = ToPosition(key);
        //        // Предполагаю, что начальная разметка массива - нули
        //        starts[position] += 1;
        //    }
        //    // Заполнение начал диапазонов
        //    int sum = 0;
        //    for (int i = 0; i < n_scale; i++)
        //    {
        //        int num_els = starts[i];
        //        starts[i] = sum;
        //        sum += num_els;
        //    }
        //    Func<object, Diapason> GetDia = k =>
        //    {
        //        int key = (int)k;
        //        if (key > max - 16)
        //        {

        //        }
        //        int ind = ToPosition(key);
        //        if (ind < 0 || ind >= n_scale)
        //        {
        //            return Diapason.Empty;
        //        }
        //        else
        //        {
        //            int sta = starts[ind];
        //            int num = ind < n_scale - 1 ? starts[ind + 1] - sta : keys.Length - sta;
        //            return new Diapason() { start = sta, numb = num };
        //        }
        //    };
        //    return GetDia;
        //}
    }
}
