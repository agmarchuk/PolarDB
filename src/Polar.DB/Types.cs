using System;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Polar.DB
{
    /// <summary>
    /// Виды примитивных типов и структуризаторов
    /// </summary>
    public enum PTypeEnumeration
    {
        none, boolean, character, integer, longinteger, real, fstring, sstring, record, sequence, union,
        @byte
        , objPair
    }
    /// <summary>
    /// Базовый класс, задающий тип
    /// </summary>
    public class PType
    {
        public PType(PTypeEnumeration vid)
        {
            this.vid = vid;
        }
        private PTypeEnumeration vid;
        public PTypeEnumeration Vid { get { return vid; } }
        /// <summary>
        /// Процедура трансляции типа, запускается по необходимости. 
        /// При трансляции определяется размер головы и есть ли у типа хвост
        /// </summary>
        public virtual void Translate() { }
        /// <summary>
        /// Размер в байтах головы данного типа (для записи, вычисляется рекурсивно)
        /// </summary>
        public virtual int HeadSize
        {
            get
            {
                switch (Vid)
                {
                    case PTypeEnumeration.none: return 0;
                    case PTypeEnumeration.boolean: return 1;
                    case PTypeEnumeration.character: return 2;
                    case PTypeEnumeration.integer: return 4; // 32-разрядные
                    case PTypeEnumeration.longinteger: return 8; // длинные
                    case PTypeEnumeration.real: return 8;
                    case PTypeEnumeration.fstring: return ((PTypeFString)this).Size;
                    case PTypeEnumeration.sstring: return 12;
                    case PTypeEnumeration.sequence: return 24; // длина (число элементов), выделенный объем, ссылка
                    case PTypeEnumeration.union: return 9; // тег, ссылка
                    case PTypeEnumeration.@byte: return 1;
                    case PTypeEnumeration.objPair: return 16; // ссылка, ссылка
                    default: return -1;
                }
            }
        }
        /// <summary>
        /// Определяет является ли тип атомарным, т.е. none, boolean, character, integer, longinteger, real
        /// </summary>
        public bool IsAtom
        {
            get
            {
                switch (Vid)
                {
                    case PTypeEnumeration.none: return true;
                    case PTypeEnumeration.boolean: return true;
                    case PTypeEnumeration.character: return true;
                    case PTypeEnumeration.integer: return true; // 32-разрядные
                    case PTypeEnumeration.longinteger: return true; // длинные
                    case PTypeEnumeration.real: return true;
                    case PTypeEnumeration.fstring: return false;
                    case PTypeEnumeration.sstring: return false;
                    case PTypeEnumeration.record: return false;
                    case PTypeEnumeration.sequence: return false; // длина (число элементов), выделенный объем, ссылка
                    case PTypeEnumeration.union: return false; // тег, ссылка
                    case PTypeEnumeration.@byte: return true;
                    case PTypeEnumeration.objPair: return false; // ссылка, ссылка
                    default: return false;
                }
            }
        }
        /// <summary>
        /// Определяет, что у типа нет (не может быть) хвоста
        /// </summary>
        public bool HasNoTail
        {
            get
            {
                switch (Vid)
                {
                    case PTypeEnumeration.none: return true;
                    case PTypeEnumeration.boolean: return true;
                    case PTypeEnumeration.character: return true;
                    case PTypeEnumeration.integer: return true; // 32-разрядные
                    case PTypeEnumeration.longinteger: return true; // длинные
                    case PTypeEnumeration.real: return true;
                    case PTypeEnumeration.fstring: return true;
                    case PTypeEnumeration.sstring: return false;
                    case PTypeEnumeration.record: return ((PTypeRecord)this).Fields.Select(pair => pair.Type).All(tp => tp.HasNoTail);
                    case PTypeEnumeration.sequence: return false; // длина (число элементов), выделенный объем, ссылка
                    case PTypeEnumeration.union: return false; // тег, ссылка
                    case PTypeEnumeration.@byte: return true;
                    case PTypeEnumeration.objPair: return false; // ссылка, ссылка
                    default: return false;
                }
            }
        }
        private static int ToInt(PTypeEnumeration pte)
        {
            switch (pte)
            {
                case PTypeEnumeration.none: return 0;
                case PTypeEnumeration.boolean: return 1;
                case PTypeEnumeration.character: return 2;
                case PTypeEnumeration.integer: return 3;
                case PTypeEnumeration.longinteger: return 4;
                case PTypeEnumeration.real: return 5;
                case PTypeEnumeration.fstring: return 6;
                case PTypeEnumeration.sstring: return 7;
                case PTypeEnumeration.record: return 8;
                case PTypeEnumeration.sequence: return 9;
                case PTypeEnumeration.union: return 10;
                case PTypeEnumeration.@byte: return 11;
                case PTypeEnumeration.objPair: return 12;
                default: return -1;
            }
        }
        //TODO: Добавление уровня - неправильный путь борьбы с рекурсивными типами, но пока ...
        //TODO: А еще я похоже "напортачил" с вариантом (растущих) последовательностей ...
        public object ToPObject(int level)
        {
            if (level < 0) return null;
            switch (this.vid)
            {
                case PTypeEnumeration.fstring:
                    {
                        return new object[] { PType.ToInt(this.vid), ((PTypeFString)this).Size };
                    }
                case PTypeEnumeration.record:
                    {
                        PTypeRecord ptr = (PTypeRecord)this;
                        var query = ptr.Fields.Select(pair => new object[] { pair.Name, pair.Type.ToPObject(level - 1) }).ToArray();
                        return new object[] { PType.ToInt(this.vid), query };
                    }
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence pts = (PTypeSequence)this;
                        return new object[] { PType.ToInt(this.vid),
                        new object[] {
                            new object[] {"growing", new object[] { PType.ToInt(PTypeEnumeration.boolean), null } },
                            new object[] {"Type", pts.ElementType.ToPObject(level - 1) } } };
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion ptu = (PTypeUnion)this;
                        var query = ptu.Variants.Select(pair => new object[] { pair.Name, pair.Type.ToPObject(level - 1) }).ToArray();
                        return new object[] { PType.ToInt(this.vid), query };
                    }
                default: return new object[] { PType.ToInt(this.vid), null };
            }
        }
        public static PType FromPObject(object po)
        {
            object[] uni = (object[])po;
            int tg = (int)uni[0];
            switch (tg)
            {
                case 0: return new PType(PTypeEnumeration.none);
                case 1: return new PType(PTypeEnumeration.boolean);
                case 2: return new PType(PTypeEnumeration.character);
                case 3: return new PType(PTypeEnumeration.integer);
                case 4: return new PType(PTypeEnumeration.longinteger);
                case 5: return new PType(PTypeEnumeration.real);
                case 6: return new PType(PTypeEnumeration.fstring);
                case 7: return new PType(PTypeEnumeration.sstring);
                case 8:
                    {
                        object[] fields_def = (object[])uni[1];
                        var query = fields_def.Select(fd =>
                        {
                            object[] f = (object[])fd;
                            return new NamedType((string)f[0], FromPObject(f[1]));
                        });
                        PTypeRecord rec = new PTypeRecord(query.ToArray());
                        return rec;
                    }
                case 9:
                    {
                        object[] growing_type = (object[])uni[1];
                        return new PTypeSequence(FromPObject(growing_type[1]), (bool)growing_type[0]);
                    }
                // case 10: не реализован вариант объединения
                case 11:
                    {
                        return new PType(PTypeEnumeration.@byte);
                    }
                // case 12: не реализован вариант объектной пары
                default:
                    {
                        throw new Exception("unknown tag for pobject");
                    }
            }
        }
        private static PTypeUnion ttype;
        public static PType TType { get { return ttype; } }
        static PType()
        {
            ttype = new PTypeUnion();
            ttype.variants =
            new NamedType[] {
            new NamedType("none", new PType(PTypeEnumeration.none)),
            new NamedType("boolean", new PType(PTypeEnumeration.none)),
            new NamedType("character", new PType(PTypeEnumeration.none)),
            new NamedType("integer", new PType(PTypeEnumeration.none)),
            new NamedType("longinteger", new PType(PTypeEnumeration.none)),
            new NamedType("real", new PType(PTypeEnumeration.none)),
            new NamedType("fstring", new PType(PTypeEnumeration.integer)),
            new NamedType("sstring", new PType(PTypeEnumeration.none)),
            new NamedType("record",
                new PTypeSequence(
                    new PTypeRecord(
                        new NamedType("Name", new PType(PTypeEnumeration.sstring)),
                        new NamedType("Type", ttype)))),
            //new NamedType("sequence", ttype),
            new NamedType("sequence",
                new PTypeRecord(
                    new NamedType("growing", new PType(PTypeEnumeration.boolean)),
                    new NamedType("Type", ttype))),
            new NamedType("union",
                new PTypeSequence(
                    new PTypeRecord(
                        new NamedType("Name", new PType(PTypeEnumeration.sstring)),
                        new NamedType("Type", ttype)))),
            new NamedType("byte", new PType(PTypeEnumeration.@byte))
                //,
                //new NamedType("objpair", new PTypeRecord(
                //    )),
                // Нет объектной пары
            };
        }
        public string Interpret(object v, bool withfieldnames = false)
        {
            switch (this.vid)
            {
                case PTypeEnumeration.none: return "";
                case PTypeEnumeration.boolean: return ((bool)v).ToString();
                case PTypeEnumeration.character: return "'" + ((char)v).ToString() + "'"; // Нужно учесть специальные символы
                case PTypeEnumeration.integer: return ((int)v).ToString();
                case PTypeEnumeration.longinteger: return ((long)v).ToString();
                case PTypeEnumeration.real: return ((double)v).ToString("G", CultureInfo.InvariantCulture);
                case PTypeEnumeration.fstring: return "\"" + ((string)v).Replace("\"", "\\\"") + "\"";
                case PTypeEnumeration.sstring: return "\"" + ((string)v).Replace("\"", "\\\"") + "\"";
                case PTypeEnumeration.record:
                    {
                        PTypeRecord ptr = (PTypeRecord)this;
                        object[] arr = (object[])v;
                        StringBuilder sb = new StringBuilder();
                        sb.Append('{');
                        for (int i = 0; i < ptr.Fields.Length; i++)
                        {
                            if (i > 0) { sb.Append(','); }
                            if (withfieldnames)
                            {
                                sb.Append(ptr.Fields[i].Name);
                                sb.Append(':');
                            }

                            sb.Append(ptr.Fields[i].Type.Interpret(arr[i]));
                        }
                        sb.Append('}');
                        return sb.ToString();
                    }
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence pts = (PTypeSequence)this;
                        PType tel = pts.ElementType;
                        object[] arr = (object[])v;
                        StringBuilder sb = new StringBuilder();
                        sb.Append('[');
                        for (int i = 0; i < arr.Length; i++)
                        {
                            if (i > 0) { sb.Append(','); }
                            sb.Append(tel.Interpret(arr[i]));
                        }
                        sb.Append(']');
                        return sb.ToString();
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion ptu = (PTypeUnion)this;
                        object[] arr = (object[])v;
                        if (arr.Length != 2) throw new Exception("incorrect data for union");
                        int tag = (int)arr[0];
                        if (tag < 0 || tag >= ptu.Variants.Length) throw new Exception("incorrect data for union");
                        NamedType nt = ptu.Variants[tag];
                        return nt.Name + "^" + nt.Type.Interpret(arr[1]);
                    }
                case PTypeEnumeration.@byte:
                    return ((byte)v).ToString();
                // не реализован вариант объектной парыЫ
                default: throw new Exception("Can't interpret value by type");
            }
        }
        //public object Parse(string text)
        //{
        //}
        //private object _parse(string text, int pos)
        //{
        //    if (this.vid == PTypeEnumeration.none) return null; // Это действие не зависит от позиции, в остальном, надо ее корректировать
        //    char c;
        //    while (char.IsWhiteSpace(c = text[pos]) || c == ',') pos++; // теперь будет новая лексема
        //    switch (this.vid)
        //    {
        //        case PTypeEnumeration.boolean: text.StartsWith(
        //        default: throw new Exception("can't parse string");
        //    }
        //}
    }
    /// <summary>
    /// Именованный тип
    /// </summary>
    public struct NamedType
    {
        public string Name;
        public PType Type;
        public NamedType(string name, PType tp)
        {
            this.Name = name;
            this.Type = tp;
        }
    }
    public class PTypeFString : PType
    {
        public PTypeFString(int length) : base(PTypeEnumeration.fstring)
        {
            this.length = length;
        }
        private int length;
        public int Size { get { return length * 2; } }
        public int Length { get { return length; } }
    }
    /// <summary>
    /// класс типа записи, наследуется от типа PType 
    /// </summary>
    public class PTypeRecord : PType
    {
        public PTypeRecord(params NamedType[] fields)
            : base(PTypeEnumeration.record)
        {
            this.fields = fields;
        }
        private int size = -1;
        private NamedType[] fields;
        public NamedType[] Fields { get { return fields; } }
        public override int HeadSize
        {
            get
            {
                if (size == -1) Translate();
                return size;
            }
        }
        public override void Translate()
        {
            if (fields == null) throw new Exception("VType Err: no fields in record def");
            size = 0;
            foreach (var field_def in fields)
            {
                //field_def.Type.Translate();
                size += field_def.Type.HeadSize;
            }
        }
    }
    /// <summary>
    /// класс типа последовательности, наследуется от типа PType
    /// </summary>
    public class PTypeSequence : PType
    {
        public PTypeSequence(PType elementtype, bool growing = false)
            : base(PTypeEnumeration.sequence)
        {
            this.elementtype = elementtype;
            //this.style = style;
        }
        private PType elementtype;
        public PType ElementType { get { return elementtype; } }
        private bool growing = false;
        public bool Growing { get { return growing; } }
        public override void Translate()
        {
            elementtype.Translate();
        }
    }
    /// <summary>
    /// класс типа объединения, наследуется от типа PType
    /// </summary>
    public class PTypeUnion : PType
    {
        public PTypeUnion(params NamedType[] variants)
            : base(PTypeEnumeration.union)
        {
            this.variants = variants;
        }
        internal NamedType[] variants = null;
        public NamedType[] Variants { get { return variants; } set { variants = value; } }
        public override void Translate()
        {
            if (variants == null) throw new Exception("VType Err: no variants in union def");
            foreach (var variant in variants)
            {
                variant.Type.Translate();
            }
        }
    }
}

