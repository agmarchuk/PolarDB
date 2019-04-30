using System;
using System.Collections.Generic;
using System.Text;

namespace Polar.DB
{
    public interface IIndexImmutable
    {
        void Clear();
        void Build();
        void Refresh();
    }
}
