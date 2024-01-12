using System;

namespace Polar.DB
{
    public struct Diapason
    {
        public long start, numb; // Инициализируются нулевые значения полей
        public static Diapason Empty { get { return new Diapason() { numb = 0, start = Int64.MinValue }; } }
        public bool IsEmpty() { return numb <= 0; }
    }
}
