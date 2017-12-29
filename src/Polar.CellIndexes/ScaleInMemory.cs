using System;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class ScaleInMemory
    {
        // Есть последовательность, которую надо индексировать
        private PaEntry sequence;
        // Есть диапазон индексирования
        private long start, number;
        // Есть функция, задающая на элементах ключевое значение (пока) целого типа
        private Func<object, int> KeyFunction;
        // Есть размер массива шкалы
        private int n_scale;

        // Есть конструктор, задающий все предыдущее
        public ScaleInMemory(PaEntry seq, long start, long number, Func<object, int> keyFunction, int n_scale)
        {
            this.sequence = seq;
            this.start = start;
            this.number = number;
            this.KeyFunction = keyFunction;
            this.n_scale = n_scale;
        }

        // Есть массив шкалы, минимальное и максмальное значения, функция отображения ключа в позицию массива шкалы
        private long[] starts;
        private int min, max;
        private Func<int, int> ToPosition { get; set; }

        // Есть построитель, который все это инициирует и строит
        public void Build()
        {
            min = KeyFunction(sequence.Element(start).Get());
            max = KeyFunction(sequence.Element(start + number - 1).Get()); //TODO: Что-то надо делать при number == 0
            // Особый случай, когда n_scale < 1 или min == max. Тогда делается одна ячейка и особая функция
            if (n_scale < 1 || min == max)
            {
                n_scale = 1;
                starts = new long[1];
                starts[0] = start;
                ToPosition = (int key) => 0;
            }
            else
            {
                starts = new long[n_scale];
                ToPosition = (int key) => (int)(((long)key - (long)min) * (long)(n_scale - 1) / (long)((long)max - (long)min));
            }
            // Заполнение количеств элементов в диапазонах
            foreach (var ob in sequence.ElementValues(start, number))
            {
                int key = KeyFunction(ob);
                int position = ToPosition(key);
                // Предполагаю, что начальная разметка массива - нули
                starts[position] += 1;
            }
            // Заполнение начал диапазонов
            long sum = start;
            for (int i = 0; i < n_scale; i++)
            {
                long num_els = starts[i];
                starts[i] = sum;
                sum += num_els;
            }
        }

        public Diapason GetDiapason(int key)
        {       
            if(ToPosition==null)
                return Diapason.Empty;
            int ind = ToPosition(key);
            if (ind < 0 || ind >= n_scale)
            {
                return Diapason.Empty;
            }
            else
            {
                long sta = starts[ind];
                long num = ind < n_scale - 1 ? starts[ind + 1] - sta : number-sta+start;
                return new Diapason() { start = sta, numb = num };
            }
        }
    }
}
