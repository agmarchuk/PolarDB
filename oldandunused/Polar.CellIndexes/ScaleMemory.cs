using System;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class ScaleMemory : IScale
    {
        private long index_cell_length;
        private long n = 0; // размер шкалы
        private long min, max;
        private PaCell index_cell;
        public PaCell IndexCell { get { return index_cell; } set { index_cell = value; } }
        public ScaleMemory() { }
        public ScaleMemory(long n) { this.n = n; }
        private Func<int, int> ToPosition { get; set; }
        //private Diapason[] diapasons;
        private long[] starts;
        public void Build()
        {
            if (n == 0) Build(index_cell.Root.Count()/32);
            else Build(n);
        }
        public void Build(long n)
        {
            this.n = n;
            index_cell_length = index_cell.Root.Count();
            //this.index_cell = index_cell;
            // Вычисление минимума и максимума
            min = (int)index_cell.Root.Element(0).Field(0).Get();
            max = (int)index_cell.Root.Element(index_cell.Root.Count()-1).Field(0).Get();
            //diapasons = new Diapason[n];
            starts = new long[n];
            ToPosition = (int key) => (int)(((long)key - min) * (long)(n - 1) / (max - min));
            // Заполнение количеств элементов в диапазонах
            index_cell.Root.Scan((long off, object val) =>
            {
                object[] pair = (object[])val;
                int position = ToPosition((int)pair[0]);
                // Предполагаю, что начальная разметка диапазона - нули
                //diapasons[position].numb += 1;
                // Предполагаю, что начальная разметка массива - нули
                starts[position] += 1;
                return true;
            });
            // Заполнение начал диапазонов
            long sum = 0;
            for (int i = 0; i < n; i++)
            {
                //diapasons[i].start = sum;
                //sum += diapasons[i].numb;
                long start = sum;
                sum += starts[i];
                starts[i] = start;
            }
        }
        public Diapason GetDiapason(int key)
        {
            int ind = ToPosition(key);
            if (ind < 0 || ind >= n)
            {
                return new Diapason() { start = 0, numb = 0 };
            }
            else
            {
                //return diapasons[ind];
                long start = starts[ind];
                long number = ind < n - 1 ? starts[ind + 1] - start : index_cell_length - start;
                return new Diapason() { start = start, numb = number };
            }
        }
        public void Warmup() {  }
        public void ActivateCache() { }
    }
}
