using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Cells
{
    /// <summary>
    /// Структура PValue служит для выдачи из входов PaEntry и PxEntry объектных значений, сопровождаемых типом и offset'ом хранения
    /// </summary>
    public struct PValue
    {
        // Может не нужно так сильно упаковывать значение?
        private long offset;
        public long Offset { get { return offset; } }
        private PType tp;
        public PType Type { get { return tp; } }
        private object value;
        public object Value { get { return value; } }
        public PValue(PType tp, long offset, object value)
        {
            this.tp = tp;
            this.offset = offset;
            this.value = value;
        }
    }

}
