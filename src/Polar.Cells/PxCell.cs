using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Cells
{
    public class PxCell : PCell
    {
        public PxCell(PType typ, string filePath, bool readOnly = true)
            : base(typ, true, filePath, readOnly) 
        {
            if (IsEmpty) Clear();
        }
        //private long freespace;
        public void Fill(object data)
        {
            // надо почистить голову
            freespace = this.dataStart + this.Type.HeadSize;
            this.Restart();
            Fill(this.dataStart, this.Type, data);
            //bw.Flush();
            //fs.Flush();
            this.Flush();
        }
        public PxEntry Root { get { /*this.Restart();*/ return new PxEntry(this.Type, this.dataStart, this); } }
        // предполагается, что pos указывает на участок памяти, достаточный для записи головы значения
        private void Fill(long pos, PType tp, object data)
        {
            if (fs.Position != pos) fs.Position = pos; //fs.Seek(pos, SeekOrigin.Begin);
            switch (tp.Vid)
            {
                case PTypeEnumeration.none: break;
                case PTypeEnumeration.boolean:
                    bw.Write((bool)data);
                    break;
                case PTypeEnumeration.character:
                    char ch = (char)data;
                    var cc = ch - '\0';
                    //char.ConvertToUtf32(
                    bw.Write((ushort)cc);
                    break;
                case PTypeEnumeration.integer:
                    bw.Write((int)data);
                    break;
                case PTypeEnumeration.longinteger:
                    bw.Write((long)data);
                    break;
                case PTypeEnumeration.real:
                    bw.Write((double)data);
                    break;
                case PTypeEnumeration.@byte:
                    bw.Write((byte)data);
                    break;
                case PTypeEnumeration.sstring:
                    {
                        string s = (string)data;
                        bw.Write((int)s.Length);
                        long pointer = Int64.MaxValue;
                        if (s.Length > 0)
                        { // захватить память
                            pointer = freespace;
                            freespace += s.Length * 2;
                        }
                        bw.Write(pointer);
                        if (s.Length > 0)
                        {
                            byte[] bytes = Encoding.Unicode.GetBytes(s);
                            if (bytes.Length != s.Length * 2) throw new Exception("Assert Error in Fill");
                            if (fs.Position != pointer) fs.Position = pointer;
                            bw.Write(bytes);
                        }

                    }
                    break;
                case PTypeEnumeration.record:
                    {
                        PTypeRecord tr = (PTypeRecord)tp;
                        long shift = 0L;
                        for (int i = 0; i < tr.Fields.Length; i++)
                        {
                            var t = tr.Fields[i].Type;
                            Fill(pos + shift, t, ((object[])data)[i]);
                            shift += t.HeadSize; // ненужные дествия для последнего прохода цикла
                        }
                    }
                    break;
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence ts = (PTypeSequence)tp;
                        PType tpe = ts.ElementType;
                        long nelements = ((object[])data).Length;
                        bw.Write(nelements);
                        long memsize = Int64.MaxValue; // это для того, чтобы всегда читать и писать
                        long pointer = Int64.MaxValue; // аналогично
                        if (nelements > 0)
                        { // захватить память
                            memsize = nelements * tpe.HeadSize;
                            pointer = freespace;
                            freespace += memsize;
                        }
                        bw.Write(memsize);
                        bw.Write(pointer);

                        // Внешний уровень определяем по позиции указателя
                        if (this.fs.Position == this.dataStart)
                        {
                            this.nElements = nelements;
                        }

                        object[] els = (object[])data;
                        // Расписываем элементы
                        for (long ii = 0; ii < els.Length; ii++) Fill(pointer + ii * tpe.HeadSize, tpe, els[ii]);
                    }
                    break;
                case PTypeEnumeration.union:
                    {
                        PTypeUnion tu = (PTypeUnion)tp;
                        object[] upair = (object[])data;
                        int tag = (int)upair[0];
                        PType tel = tu.Variants[tag].Type;
                        bw.Write((byte)tag);
                        // либо значение, либо ссылка на новую память
                        if (tel.IsAtom)
                        {
                            //Fill(pos + 1, tel, upair[1]);
                            if (upair[1] == null) bw.Write(-1L);
                            else
                            { // Это точно не правильно, надо сделать аккуратнее
                                bw.Write((long)upair[1]); // ??? Это вряд ли правильно, поскольку атомы бывают разных типов
                            }
                        }
                        else
                        {
                            long point = freespace;
                            freespace += tel.HeadSize;
                            bw.Write(point);
                            // введем значение элемента
                            Fill(point, tel, upair[1]);
                        }
                    }
                    break;
                default: throw new Exception("Err in Fill: unknown Vid of vtype [" + tp.Vid + "]");
            }
        }
        // Процедура будет заполнять данными файл, причем считается, что offset уже установлен, а запись идет подряд
        // через бинарный райтер. Процедура разбивается на две. Одна FillHead записывает голову объекта в текущую позицию,
        // определяет свободное место и пара объект-место ставится в конец очереди. Потом из очереди берутся пары, 
        // проверяется, что offset тот, каторый ожидался и пишется FillTail
        public void Fill2(object data)
        {
            freespace = this.dataStart;
            fs.Position = this.dataStart;
            Queue<VTO> vtoList = new Queue<VTO>();
            vtoList.Enqueue(new VTO(data, this.Type, freespace) { makehead = true });
            freespace += this.Type.HeadSize;
            VTO tolook;
            //long ii = 0;
            while (vtoList.Count > 0)
            {

                var listElement = vtoList.Dequeue();
                //vtoList.RemoveAt(0);
                var pos = fs.Position;
                if (pos != listElement.off) throw new Exception("Wrong order of elements in vtoList");
                //if (ii < 3 || ii % 100000 == 0) { Console.WriteLine("Before Fill: " + pos + " " + vtoList.LongCount()); } ii++;
                if (listElement.makehead) FillHead(listElement.value, listElement.ty, vtoList);
                else
                {
                    FillTail(listElement.value, listElement.ty, vtoList);
                }
                tolook = listElement;
            }
            bw.Flush();
            fs.Flush();
        }
        // Для головы место уже выделено
        internal void FillHead(object valu, PType typ, Queue<VTO> vtoList)
        {
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: break;
                case PTypeEnumeration.boolean: bw.Write((bool)valu); break;
                case PTypeEnumeration.character:
                    {
                        char ch = (char)valu;
                        var cc = ch - '\0';
                        //char.ConvertToUtf32(
                        bw.Write((ushort)cc);
                        break;
                    }
                case PTypeEnumeration.integer: bw.Write((int)valu); break;
                case PTypeEnumeration.longinteger: bw.Write((long)valu); break;
                case PTypeEnumeration.real: bw.Write((double)valu); break;
                case PTypeEnumeration.@byte: bw.Write((byte)valu); break;
                case PTypeEnumeration.fstring:
                    {
                        string s = (string)valu;
                        int size = ((PTypeFString)typ).Size;
                        byte[] arr = new byte[size];
                        if (s.Length * 2 > size) s = s.Substring(0, size / 2);
                        var qu = System.Text.Encoding.Unicode.GetBytes(s, 0, s.Length, arr, 0);
                        bw.Write(arr);
                    }
                    break;
                case PTypeEnumeration.sstring:
                    {
                        string s = (string)valu;
                        int len = s.Length;
                        bw.Write(len);
                        long off = this.freespace;
                        this.freespace += 2 * len;
                        bw.Write(off);
                        if (len > 0)
                        { // Если есть строка, ставим в очередь
                            vtoList.Enqueue(new VTO(valu, typ, off));
                        }
                    }
                    break;
                case PTypeEnumeration.record:
                    {
                        PTypeRecord mtr = (PTypeRecord)typ;
                        object[] arr = (object[])valu;
                        if (arr.Length != mtr.Fields.Length) throw new Exception("record has wrong number of fields");
                        for (int i = 0; i < arr.Length; i++)
                        {
                            FillHead(arr[i], mtr.Fields[i].Type, vtoList);
                        }
                    }
                    break;
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        // Пишем голову последовательности
                        long llen = ((object[])valu).Length;
                        long off = this.freespace;

                        // Внешний уровень определяем по позиции указателя
                        if (this.fs.Position == this.dataStart)
                        {
                            this.nElements = llen;
                        }

                        bw.Write(llen);
                        bw.Write(0L);
                        bw.Write(off);
                        this.freespace += tel.HeadSize * llen;
                        // Если есть хвост, ставим в очередь
                        if (llen > 0) vtoList.Enqueue(new VTO(valu, typ, off));
                    }
                    break;
                case PTypeEnumeration.union:
                    {
                        object[] arr = (object[])valu;
                        int tag = (int)arr[0];
                        PTypeUnion mtu = (PTypeUnion)typ;
                        PType tel = mtu.Variants[tag].Type;
                        // Пишем голову
                        bw.Write((byte)(int)arr[0]);
                        if (tel.IsAtom)
                        {
                            if (arr[1] == null) bw.Write(-1L);
                            else
                            {
                                WriteAtomAsLong(tel.Vid, arr[1]);
                            }
                        }
                        else
                        {
                            long off = freespace;
                            freespace += tel.HeadSize;
                            bw.Write(off);
                            vtoList.Enqueue(new VTO(arr[1], tel, off) { makehead = true });
                        }
                    }
                    break;
                default: throw new Exception("unexpected type");
            }
        }

        internal void WriteAtomAsLong(PTypeEnumeration vid, object data)
        {
            switch (vid)
            {
                case PTypeEnumeration.none: break;
                case PTypeEnumeration.boolean:
                    {
                        bw.Write((bool)data);
                        bw.Write(new byte[] { 0, 0, 0, 0, 0, 0, 0 });
                        break;
                    }
                case PTypeEnumeration.character:
                    {
                        bw.Write(Encoding.Unicode.GetBytes(new[] { (char)data }));
                        bw.Write(new byte[] { 0, 0, 0, 0, 0, 0 });
                        break;
                    }
                case PTypeEnumeration.integer:
                    {
                        bw.Write((int)data);
                        bw.Write(0);
                        break;
                    }
                case PTypeEnumeration.longinteger: bw.Write((long)data); break;
                case PTypeEnumeration.real: bw.Write((double)data); break;
                case PTypeEnumeration.@byte: bw.Write((byte)data); break;
            }

        }

        internal object ReadAtomFromLong(PTypeEnumeration vid, long offset)
        {
            SetOffset(offset);
            switch (vid)
            {
                case PTypeEnumeration.boolean:
                    {
                        var b = br.ReadBoolean();
                        SetOffset(offset + 7);
                        return b;
                    }
                case PTypeEnumeration.character:
                    {
                        var ch = Encoding.Unicode.GetString(br.ReadBytes(2), 0, 2)[0];
                        SetOffset(offset + 6);
                        return ch;
                    }
                case PTypeEnumeration.integer:
                    {
                        var i = br.ReadInt32();
                        SetOffset(offset + 4);
                        return i;
                    }
                case PTypeEnumeration.longinteger:
                    {
                        return br.ReadInt64();
                    }
                case PTypeEnumeration.real:
                    {
                        return br.ReadDouble();
                    }
                case PTypeEnumeration.@byte:
                    {
                        return br.ReadByte();
                    }
            }
            return null;
        }

        // Голова построена, для хвоста место выдалено место уже выделено и оно "под головкой"
        internal void FillTail(object valu, PType typ, Queue<VTO> vtoList)
        {
            switch (typ.Vid)
            {
                case PTypeEnumeration.sstring:
                    {
                        string str = (string)valu;
                        int len = str.Length;
                        bw.Write(Encoding.Unicode.GetBytes(str));
                    }
                    break;
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        // Пишем массив последовательности
                        long llen = ((object[])valu).Length;
                        // Если строка, то специальная обработка
                        object[] arr = (object[])valu;
                        foreach (object va in arr)
                        {
                            FillHead(va, tel, vtoList);
                        }
                    }
                    break;
                default: throw new Exception("unexpected type in FillTail");
            }
        }
    }
    internal class VTO
    {
        public object value;
        public PType ty;
        public long off;
        public bool makehead = false;
        public VTO(object v, PType t, long o)
        {
            this.value = v;
            this.ty = t;
            this.off = o;
        }
    }
}
