using System;
using System.Collections.Generic;
using System.Text;

namespace Universal
{
    public interface IIndex
    {
        Sequence BearingSequence { get; set; }
        //Func<object, object> keyFunc { get; set; }
        //Func<object, int> Hash32 { get; set; }
        void Clear();
        void AppendPosition(long offset, object element);
        void Flush();
        void Build();
        void Prepare();
    }
}
