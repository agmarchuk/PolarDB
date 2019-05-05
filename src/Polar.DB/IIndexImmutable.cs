using System;
using System.Collections.Generic;
using System.Text;

namespace Polar.DB
{
    public interface IIndex
    {
        void Clear();
        void Build();
        void Refresh();
        //IBearing Bearing { get; set; }
    }
    public interface IIndexImmutable
    {
        void Clear();
        void Build();
        void Refresh();
    }
}
