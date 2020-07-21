using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Common
{
    public class CommonField : IField
    {
        public virtual FieldType Type { get => throw new NotImplementedException(); }
        public string LabelName { get; set; }
        public string Name { get; set; }
        public object Value { get; set; }
        public object DefaultValue { get; set; }
        public CommonField(string label, string name)
        {
            LabelName = label;
            Name = name;
        }
    }
    public class NumericField : CommonField
    {
        public override FieldType Type { get => FieldType.Long; }
        public NumericField(string label, string name) : base(label, name) { }

    }
    public class StringField : CommonField
    {
        public override FieldType Type { get => FieldType.String; }
        public StringField(string label, string name) : base(label, name) { }
    }
    public class DateField : CommonField
    {
        public override FieldType Type { get => FieldType.DateTime; }
        public DateField(string label, string name) : base(label, name) { }
    }
    public class BooleanField : CommonField
    {
        public override FieldType Type { get => FieldType.Boolean; }
        public BooleanField(string label, string name) : base(label, name) { }
    }
}
