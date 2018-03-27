using System;
using System.Collections.Generic;
using System.Text;
using Polar.DB;

namespace Polar.Datanode
{
    public partial class Datanode
    {
        private Polar.PagedStreams.StreamStorage sstorage;
        private PType[] element_types; // по одному типу на таблицу
        // Функции преобразований (2 - ->) tab, lay, sec, por - таблица, слой, секция, порция
        private Func<int, int, int> tablay2node;
        private Func<int, int> tab2n;
        private Func<int, PType> tab2t;
        private Func<int, int[]> tab2indxs; // у таблицы есть индексы (получаем массив колонок)

    }
}
