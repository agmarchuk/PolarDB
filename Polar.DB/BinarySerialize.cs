using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
//using PolarDB;

namespace Polar.DB
{
    public class ByteFlow
    {
        public static void Serialize(BinaryWriter bw, object v, PType tp)
        {
            switch (tp.Vid)
            {
                case PTypeEnumeration.none: { return; }
                case PTypeEnumeration.boolean: { bw.Write((bool)v); return; }
                case PTypeEnumeration.@byte: { bw.Write((byte)v); return; }
                case PTypeEnumeration.character: { bw.Write((char)v); return; }
                case PTypeEnumeration.integer: { bw.Write((int)v); return; }
                case PTypeEnumeration.longinteger: { bw.Write((long)v); return; }
                case PTypeEnumeration.real: { bw.Write((double)v); return; }
                case PTypeEnumeration.sstring: { if (v == null) v = ""; bw.Write((string)v); return; }
                case PTypeEnumeration.record:
                    {
                        object[] rec = (object[])v;
                        PTypeRecord tp_rec = (PTypeRecord)tp;
                        if (rec.Length != tp_rec.Fields.Length) throw new Exception("Err in Serialize: wrong record field number");
                        for (int i = 0; i < rec.Length; i++)
                        {
                            Serialize(bw, rec[i], tp_rec.Fields[i].Type);
                        }
                        return;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PType tp_element = ((PTypeSequence)tp).ElementType;
                        object[] elements = (object[])v;
                        bw.Write((long)elements.Length);
                        foreach (object el in elements) Serialize(bw, el, tp_element);
                        return;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion tp_uni = (PTypeUnion)tp;
                        // тег - 1 байт
                        int tag = (int)((object[])v)[0];
                        object subval = ((object[])v)[1];
                        if (tag < 0 || tag >= tp_uni.Variants.Length) throw new Exception("Err in Serialize: wrong union tag");
                        bw.Write((byte)tag);
                        Serialize(bw, subval, tp_uni.Variants[tag].Type);
                        return;
                    }
            }
        }
        public static object Deserialize(BinaryReader br, PType tp)
        {
            switch (tp.Vid)
            {
                case PTypeEnumeration.none: { return null; }
                case PTypeEnumeration.boolean: { return br.ReadBoolean(); }
                case PTypeEnumeration.@byte: { return br.ReadByte(); }
                case PTypeEnumeration.character: { return br.ReadChar(); }
                case PTypeEnumeration.integer: { return br.ReadInt32(); }
                case PTypeEnumeration.longinteger: { return br.ReadInt64(); }
                case PTypeEnumeration.real: { return br.ReadDouble(); }
                case PTypeEnumeration.sstring: { return br.ReadString(); }
                case PTypeEnumeration.record:
                    {
                        PTypeRecord tp_rec = (PTypeRecord)tp;
                        object[] rec = new object[tp_rec.Fields.Length];
                        for (int i = 0; i < rec.Length; i++)
                        {
                            object v = Deserialize(br, tp_rec.Fields[i].Type);
                            rec[i] = v;
                        }
                        return rec;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PType tp_element = ((PTypeSequence)tp).ElementType;
                        long nelements = br.ReadInt64();
                        if (nelements < 0 || nelements > Int32.MaxValue) throw new Exception($"Err in Deserialize: sequense has too many ({nelements}) elements");
                        object[] elements = new object[nelements];
                        for (int i = 0; i<nelements; i++)
                        {
                            elements[i] = Deserialize(br, tp_element);
                        }
                        return elements;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion tp_uni = (PTypeUnion)tp;
                        // тег - 1 байт
                        int tag = br.ReadByte();
                        object subval = Deserialize(br, tp_uni.Variants[tag].Type);
                        return new object[] { tag, subval };
                    }
                default: { throw new Exception($"Err in Deserialize: unknown type variant {tp.Vid}"); }
            }
        }
    }
}
