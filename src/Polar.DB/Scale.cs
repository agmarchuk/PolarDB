﻿using System;
using System.Collections.Generic;
using System.Text;
//using Polar.DB;

namespace Polar.DB
{
    public class Scale
    {
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
