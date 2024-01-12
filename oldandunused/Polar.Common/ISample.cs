﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Common
{
    public interface ISample
    {
        void Run();
        void Clear();
        ICollection<IField> Fields { get; }
        string DiplayName { get;}
        string Name { get; set; }
    }

    public interface IField
    {
        FieldType Type { get; }
        string LabelName { get; set; }
        string Name { get; set; }
        object Value { get; set; }
        object DefaultValue {get; set;}
    }

    public enum FieldType
    {
        DateTime = 1,
        String =   2,
        Long = 3,
        Boolean = 4,
        Byte = 5
    }
}
