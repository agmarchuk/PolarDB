using System;
using System.Collections.Generic;
using System.Text;

namespace Universal
{
    public interface IIndex
    {
        Sequence BearingSequence { get; set; }
        void Clear();
        void AppendPosition(long offset, object element);
        void Flush();
        void Build();
    }
}
