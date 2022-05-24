using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Universal
{
    public struct ObjOff
    {
        public object obj;
        public long off;
        public ObjOff(object obj, long off) { this.obj = obj; this.off = off; }
    }

    public interface IUIndex
    {
        void Clear();
        void Flush();
        void Close();
        void Refresh();
        void Build();
        void OnAppendElement(IComparable key, long offset);
        //IEnumerable<ObjOff> GetAllByValue(IComparable valuesample);
        //IEnumerable<ObjOff> GetAllBySample(object sample);
    }
}
