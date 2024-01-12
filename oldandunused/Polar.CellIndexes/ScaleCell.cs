using System;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public class ScaleCell : IScale
    {
        private long n = 0; // размер шкалы
        private long min, max;
        private PaCell index_cell;
        public PaCell IndexCell {
            get { return index_cell; }
            set
            {
                index_cell = value;
                if (!scell.IsEmpty)
                {
                    this.n = this.scell.Root.Count();
                    if (index_cell.Root.Count() > 0)
                    {
                        CreateMethodToPosition();// Будет null если нет массива
                    }
                }
            } 
        }
        private PaCell scell;
        public ScaleCell(System.IO.Stream stream)
        {
            this.scell = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.longinteger)),
                stream, false);
            if (!this.scell.IsEmpty) this.n = this.scell.Root.Count();
        }
        public ScaleCell(string index_path_name) 
        { 
            //this.index_cell = index_cell;
            
            this.scell = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.longinteger)),
                index_path_name + "_scale.pac", false);
            if (!this.scell.IsEmpty) this.n = this.scell.Root.Count();
        }
        private Func<int, int> ToPosition { get; set; }
        public void Build()
        {
            
            if (index_cell.Root.Count() <= 0) return;
            
            if (n == 0) Build(index_cell.Root.Count() / 64 > 0 ? index_cell.Root.Count() / 32 : 1);
            else Build(n);
        }
        public void Build(long n)
        {
            this.n = n;
            long[] numbers = new long[n];
            CreateMethodToPosition();

            // Заполнение количеств элементов в диапазонах
            index_cell.Root.Scan((long off, object val) =>
            {
                object[] pair = (object[])val;
                int position = ToPosition((int)pair[0]);
                // Предполагаю, что начальная разметка массива - нули
                numbers[position] += 1;
                return true;
            });
            // Заполнение начал диапазонов
            this.scell.Clear();
            this.scell.Fill(new object[0]);
            long sum = 0;
            for (int i = 0; i < n; i++)
            {
                long start = sum;
                sum += numbers[i];
                scell.Root.AppendElement(start);
            }
            scell.Flush();
        }

        private void CreateMethodToPosition()
        {
            // Вычисление минимума и максимума
            min = (int) index_cell.Root.Element(0).Field(0).Get();
            max = (int) index_cell.Root.Element(index_cell.Root.Count() - 1).Field(0).Get();
            //diapasons = new Diapason[n];
            if (max == min)
                ToPosition = (int key) => key == min ? 0 : -1;
            else
                ToPosition = (int key) => (int) (((long) key - min)*(long) (n - 1)/(max - min));
        }

        public Diapason GetDiapason(int key)
        {
            if (ToPosition == null)
                return Diapason.Empty;
             int ind = ToPosition(key);
            if (ind < 0 || ind >= n)
            {
                return new Diapason() { start = 0, numb = 0 };
            }
            else
            {
                //return diapasons[ind];
                long start = (long)scell.Root.Element(ind).Get();
                long number = ind < n - 1 ? (long)scell.Root.Element(ind + 1).Get() - start : index_cell.Root.Count() - start;
                return new Diapason() { start = start, numb = number };
            }
        }
        public void Warmup()
        {
            foreach (var v in scell.Root.ElementValues()) ;
        }
        public void ActivateCache() { scell.ActivateCache(); }
    }
}
