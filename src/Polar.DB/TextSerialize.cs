using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
//using PolarDB;

namespace Polar.DB
{
    public class TextFlow
    {
        public static void Serialize(TextWriter tw, object v, PType tp)
        {
            switch (tp.Vid)
            {
                case PTypeEnumeration.none: { return; }
                case PTypeEnumeration.boolean: { tw.Write((bool)v?'t':'f'); return; }
                case PTypeEnumeration.@byte: { tw.Write(((byte)v).ToString()); return; }
                case PTypeEnumeration.character: { tw.Write((char)v); return; }
                case PTypeEnumeration.integer: { tw.Write((int)v); return; }
                case PTypeEnumeration.longinteger: { tw.Write((long)v); return; }
                case PTypeEnumeration.real: { tw.Write(((double)v).ToString("G", System.Globalization.CultureInfo.InvariantCulture)); return; }
                case PTypeEnumeration.sstring:
                    {
                        tw.Write('\"');
                        tw.Write(((string)v).Replace("\\", "\\\\").Replace("\"", "\\\""));
                        tw.Write('\"');
                        return;
                    }
                case PTypeEnumeration.record:
                    {
                        object[] rec = (object[])v;
                        PTypeRecord tp_rec = (PTypeRecord)tp;
                        if (rec.Length != tp_rec.Fields.Length) throw new Exception("Err in Serialize: wrong record field number");
                        tw.Write('{');
                        for (int i = 0; i < rec.Length; i++)
                        {
                            if (i != 0) tw.Write(',');
                            Serialize(tw, rec[i], tp_rec.Fields[i].Type);
                        }
                        tw.Write('}');
                        return;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PType tp_element = ((PTypeSequence)tp).ElementType;
                        object[] elements = (object[])v;
                        tw.Write('[');
                        bool isfirst = true;
                        foreach (object el in elements)
                        {
                            if (!isfirst) tw.Write(','); isfirst = false;
                            Serialize(tw, el, tp_element);
                        }
                        tw.Write(']');
                        return;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion tp_uni = (PTypeUnion)tp;
                        // тег - 1 байт
                        int tag = (int)((object[])v)[0];
                        object subval = ((object[])v)[1];
                        if (tag < 0 || tag >= tp_uni.Variants.Length) throw new Exception("Err in Serialize: wrong union tag");
                        tw.Write(tag);
                        tw.Write('^');
                        Serialize(tw, subval, tp_uni.Variants[tag].Type);
                        return;
                    }
            }
        }
        public static object Deserialize(TextReader tr, PType tp) { TextFlow tf = new TextFlow(tr); tf.Skip(); return tf.Des(tp); }

        // Более удобный объект для парсинга TextFlow
        private TextReader tr;
        internal TextFlow(TextReader tr) { this.tr = tr; }
        public void Skip()
        {
            while (char.IsWhiteSpace((char)tr.Peek())) tr.Read();
        }
        public bool ReadBoolean()
        {
            int c = tr.Read();
            return c == 't' ? true : false;
        }
        private string ReadWhile(Func<char, bool> yesFunc)
        {
            StringBuilder sb = new StringBuilder();
            char c;
            while (yesFunc(c = (char)tr.Peek()))
            {
                c = (char)tr.Read();
                sb.Append((char)c);
            }
            return sb.ToString();
        }
        public byte ReadByte()
        {
            string s = ReadWhile(c => { if (char.IsDigit(c)) return true; char cc = char.ToLower(c); return cc >= 'a' && cc <= 'f'; });
            return byte.Parse(s);
        }
        public char ReadChar() { return (char)tr.Read(); }
        public int ReadInt32()
        {
            int sign = 1;
            if (tr.Peek() == '-') { sign = -1; tr.Read(); }
            string s = ReadWhile(c => c >= '0' && c <= '9');
            int v = Int32.Parse(s);
            return sign * v;
        }
        public long ReadInt64()
        {
            int sign = 1;
            if (tr.Peek() == '-') { sign = -1; tr.Read(); }
            string s = ReadWhile(c => c >= '0' && c <= '9');
            long v = Int64.Parse(s);
            return sign * v;
        }
        public double ReadDouble()
        {
            // Наверное, это неправильно, но пока сойдет
            string s = ReadWhile(c => (c >= '0' && c <= '9') || c == '-' || c == 'e' || c == '.');
            double v = double.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
            return v;
        }
        public string ReadString()
        {
            StringBuilder sb = new StringBuilder();
            // Маленький конечный автомат
            // начальная точка, сюда уже не вернемся
            if (tr.Peek() != '\"') throw new Exception("Err: wrong string construction");
            int c = tr.Read();
            // Внутри строки очередной символ прочитан
            c = tr.Read();
            while (c != '\"')
            {
                if (c == '\\')
                {
                    c = tr.Read();
                    if (c == 'n') sb.Append('\n');
                    else if (c == 'r') sb.Append('\r');
                    else if (c == 't') sb.Append('\t');
                    else
                    {
                        sb.Append((char)c);
                    }
                }
                else
                {
                    sb.Append((char)c);
                }
                c = tr.Read();
            }
            return sb.ToString();
        }

        private object Des(PType tp)
        {
            switch (tp.Vid)
            {
                case PTypeEnumeration.none: { return null; }
                case PTypeEnumeration.boolean: { return ReadBoolean(); }
                case PTypeEnumeration.@byte: { return ReadByte(); }
                case PTypeEnumeration.character: { return ReadChar(); }
                case PTypeEnumeration.integer: { return ReadInt32(); }
                case PTypeEnumeration.longinteger: { return ReadInt64(); }
                case PTypeEnumeration.real: { return ReadDouble(); }
                case PTypeEnumeration.sstring: { return ReadString(); }
                case PTypeEnumeration.record:
                    {
                        PTypeRecord tp_rec = (PTypeRecord)tp;
                        object[] rec = new object[tp_rec.Fields.Length];
                        char c = (char)tr.Read();
                        if (c != '{') throw new Exception("Polar syntax error 19327");
                        for (int i = 0; i < rec.Length; i++)
                        {
                            Skip();
                            object v = Des(tp_rec.Fields[i].Type);
                            rec[i] = v;
                            if (i < rec.Length - 1)
                            {
                                Skip();
                                c = (char)tr.Read();
                                if (c != ',') throw new Exception("Polar syntax error 19329");
                            }
                            Skip();
                        }
                        c = (char)tr.Read();
                        if (c != '}') throw new Exception("Polar syntax error 19328");
                        return rec;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PType tp_element = ((PTypeSequence)tp).ElementType;
                        List<object> lsequ = new List<object>();
                        char c = (char)tr.Read();
                        if (c != '[') throw new Exception("Polar syntax error 19331");
                        while (true)
                        {
                            Skip();
                            //TODO: неудачно, что дважды проверяю и выхожу по закрывающей скобке
                            if (tr.Peek() == ']') { c = (char)tr.Read(); break; }
                            lsequ.Add(Des(tp_element));
                            Skip();
                            c = (char)tr.Read();
                            if (c == ']') break;
                            else if (c == ',') continue;
                            throw new Exception("Polar syntax error 19333");
                        }
                        if (c != ']') throw new Exception("Polar syntax error 19332");
                        object[] elements = lsequ.ToArray();
                        return elements;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion tp_uni = (PTypeUnion)tp;
                        // тег - 1 байт
                        int tag = ReadInt32();
                        Skip(); int c = tr.Read(); if (c != '^') throw new Exception("Polar syntax error 19335");
                        Skip();
                        object subval = Des(tp_uni.Variants[tag].Type);
                        return new object[] { tag, subval };
                    }
                default: { throw new Exception($"Err in Deserialize: unknown type variant {tp.Vid}"); }
            }
        }
    }
}
