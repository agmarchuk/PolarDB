using System;
using System.Collections.Generic;
using System.Text;

namespace Polar.DB
{
    public interface IIndex
    {
        void Clear();
        void Build();
        void Flush();
        void Refresh();
        //IBearing Bearing { get; set; }
        void OnAddItem(object item, long off);
        void OnDeleteItem(long off);

    }
    public interface IIndexImmutable
    {
        void Clear();
        void Build();
        void Flush();
        void Refresh();
    }
}
