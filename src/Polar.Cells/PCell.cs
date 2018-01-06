using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Cells
{
    // Структура начала ячейки: 1) 64-разрядный "секретный" код; 2) 64-разрядное значение начала поля данных
    // 3) 64-разрядное значение начала свободного пространства; 
    // 4) 8 байтов для всяких дел, 1-й байт для записи формата ячейки isFixed (1 - фиксированный, 0 - свободный).
    // 5) Объект типа, записанный в свободном формате;
    // 6) Сохраненное значение
    public abstract class PCell
    {
        // Структура записанной в прологе данных
        private const long security_cod_pac = 999111777840219834L;
        private const long security_cod_pxc = 1999111777840219834L; // OL
        protected internal long dataStart;                                   // 8L
        protected internal long freespace;                                    // 16L
        private bool isFixed; // плюс еще 7 байтов резерва          // 24L
        private PType typ;                                          // 32L
        protected internal long nElements = Int64.MinValue; // Если в ячейке хранится последовательность, то это - число элементов последовательности. Сбрасывается в файл методом Flush
        private bool readOnly;
        protected internal Stream fs;
        private FileStream filestream; // Чтобы не потерять и можно было восстановить 
        protected internal BinaryReader br;
        protected internal BinaryWriter bw = null;
        public PType Type { get { return typ; } }
        protected void Restart() { SetOffset(dataStart); } //{ fs.Position = dataStart; }
        protected internal long GetOffset() { return fs.Position; }
        protected internal void SetOffset(long offset) { if (fs.Position != offset) fs.Position = offset; }

        /// <summary>
        /// Новый вариант конструктура ячейки - на основе потока
        /// </summary>
        /// <param name="typ"></param>
        /// <param name="isFixed"></param>
        /// <param name="stream"></param>
        /// <param name="readOnly"></param>
        public PCell(PType typ, bool isFixed, Stream stream, bool readOnly)
        {
            this.isFixed = isFixed;
            this.readOnly = readOnly;
            this.typ = typ;
            //var obj_typ = typ.ToPObject(4);
            if (stream.Length == 0)
            {
                if (readOnly) throw new Exception("Can't create file in ReadOnly mode");
                this.filestream = null;
                this.fs = stream;

                //File.Open(filePath, System.IO.FileMode.Create);
                this.bw = new BinaryWriter(this.fs);
                this.bw.Write(isFixed ? security_cod_pxc : security_cod_pac);
                this.bw.Write(-1L); // Это вместо DataStart
                this.bw.Write(-1L); // Это вместо freespace
                this.bw.Write(isFixed); for (int i = 0; i < 7; i++) this.bw.Write((byte)0);
                // 
                //TODO: Пока блокирую запись типа в ячейку, но надо будет это исправить
                //this.Append(PType.TType, obj_typ);
                this.dataStart = fs.Position;
                this.freespace = this.dataStart;
                fs.Position = 8L;
                this.bw.Write(this.dataStart);
                this.bw.Write(this.freespace);
                fs.Position = this.dataStart;
            }
            else
            {
                long code = 0L;
                this.fs = stream;
                this.br = new BinaryReader(this.fs);
                if (!readOnly) this.bw = new BinaryWriter(this.fs);
                code = this.br.ReadInt64();
                if (isFixed && code == security_cod_pxc || !isFixed && code == security_cod_pac) { } // ok
                else throw new Exception("File is not PolarDB file");
                this.dataStart = this.br.ReadInt64();
                this.freespace = this.br.ReadInt64();
                //TODO: Пока тип не сохраняется и не проверяется
                //// Попробуем прочитать тип
                //this.fs.Position = 32L;
                //object vvv = ScanObject(PType.TType);
                //if (!Objects.Equvalent(obj_typ, vvv))
                //{
                //    throw new Exception("previous and current types are not equivalent");
                //}
            }
            if (br == null) this.br = new BinaryReader(this.fs);
            if (!readOnly && bw == null) this.bw = new BinaryWriter(this.fs);
            // Сформируем значение nElements
            if (typ.Vid == PTypeEnumeration.sequence && !this.IsEmpty)
            {
                this.SetOffset(this.dataStart);
                this.nElements = br.ReadInt64();
            }
        }

        public PCell(PType typ, bool isFixed, string filePath, bool readOnly)
        {
            this.isFixed = isFixed;
            this.readOnly = readOnly;
            this.typ = typ;
            //var obj_typ = typ.ToPObject(4);
            if (!File.Exists(filePath))
            {
                if (readOnly) throw new Exception("Can't create file in ReadOnly mode");
                this.filestream = new FileStream(filePath, FileMode.OpenOrCreate,
                    readOnly ? FileAccess.Read : FileAccess.ReadWrite,
                    FileShare.Read);
                this.fs = this.filestream;
                    
                    //File.Open(filePath, System.IO.FileMode.Create);
                this.bw = new BinaryWriter(this.fs);
                this.bw.Write(isFixed ? security_cod_pxc : security_cod_pac);
                this.bw.Write(-1L); // Это вместо DataStart
                this.bw.Write(-1L); // Это вместо freespace
                this.bw.Write(isFixed); for (int i=0; i<7; i++) this.bw.Write((byte)0);
                // 
                //TODO: Пока блокирую запись типа в ячейку, но надо будет это исправить
                //this.Append(PType.TType, obj_typ);
                this.dataStart = fs.Position;
                this.freespace = this.dataStart;
                fs.Position = 8L;
                this.bw.Write(this.dataStart);
                this.bw.Write(this.freespace);
                fs.Position = this.dataStart;
            }
            else
            {
                long code = 0L;
                try
                {
                    this.fs = File.Open(filePath, System.IO.FileMode.Open);
                    this.br = new BinaryReader(this.fs);
                    if (!readOnly) this.bw = new BinaryWriter(this.fs);
                    code = this.br.ReadInt64();
                }
                catch (Exception ex)
                {
                    throw new Exception("File is not PolarDB file. Message: " + ex.Message);
                }
                if (isFixed && code == security_cod_pxc || !isFixed && code == security_cod_pac) { } // ok
                else throw new Exception("File is not PolarDB file");
                this.dataStart = this.br.ReadInt64();
                this.freespace = this.br.ReadInt64();
                //TODO: Пока тип не сохраняется и не проверяется
                //// Попробуем прочитать тип
                //this.fs.Position = 32L;
                //object vvv = ScanObject(PType.TType);
                //if (!Objects.Equvalent(obj_typ, vvv))
                //{
                //    throw new Exception("previous and current types are not equivalent");
                //}
            }
            if (br == null) this.br = new BinaryReader(this.fs);
            if (!readOnly && bw == null) this.bw = new BinaryWriter(this.fs);
            // Сформируем значение nElements
            if (typ.Vid == PTypeEnumeration.sequence && !this.IsEmpty)
            {
                this.SetOffset(this.dataStart);
                this.nElements = br.ReadInt64();
            }
        }
        public bool IsEmpty { get { return this.freespace == this.dataStart; } }
        public void Clear() 
        {
            if (readOnly) throw new Exception("Can't Clear() readonly cell");
            this.freespace = this.dataStart;
            this.nElements = Int64.MinValue;
            if (this is PxCell)
            {
                int size = typ.HeadSize;
                byte[] empty = new byte[size];
                this.fs.Position = this.dataStart;
                bw.Write(empty);
                this.freespace += size;
            }
            if (typ.Vid == PTypeEnumeration.sequence && !this.IsEmpty)
            {
                this.nElements = 0;
            }
            this.fs.Position = 16L;
            this.bw.Write(this.freespace);
            bw.Flush();
            fs.Flush();
        }
        public void Close()
        {
            if (bw != null)
            {
                Flush();
            }
            //this.fs.Close();
            this.fs.Flush();
            this.fs.Dispose();
        }

        // Признак того, что последовательность требует Flush()
        internal bool toflush = false;

        public void Flush()
        {
            long pos = fs.Position;
            fs.Position = 16L;
            bw.Write(this.freespace);
            //if (typ.Vid == PTypeEnumeration.sequence && !this.IsEmpty
            //    && this.nElements >= 0L) // Возможно, избыточное правило
            //{
            //    this.fs.Position = this.dataStart;
            //    this.bw.Write(this.nElements);
            //}
            //bw.Flush();
            if (typ.Vid == PTypeEnumeration.sequence && !this.IsEmpty)
            {
                fs.Position = this.dataStart;
                bw.Write(this.nElements);
            }
            toflush = false;
            fs.Flush();
            fs.Position = pos;
        }
        /// <summary>
        /// Добавляет объектное значение в ячейку по месту текущей позиции файла
        /// </summary>
        /// <param name="typ"></param>
        /// <param name="value"></param>
        protected internal void Append(PType typ, object value)
        {
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: break;
                case PTypeEnumeration.boolean: bw.Write((bool)value); break;
                case PTypeEnumeration.character: bw.Write((char)value); break;
                case PTypeEnumeration.integer: bw.Write((int)value); break;
                case PTypeEnumeration.longinteger: bw.Write((long)value); break;
                case PTypeEnumeration.real: bw.Write((double)value); break;
                case PTypeEnumeration.@byte: bw.Write((byte)value); break;
                case PTypeEnumeration.fstring:
                    {
                        string str = (string)value;
                        int maxlength = ((PTypeFString)typ).Length;
                        if (str.Length < maxlength) maxlength = str.Length;
                        int size = ((PTypeFString)typ).Size;
                        byte[] bytes = new byte[size];
                        int nb = Encoding.Unicode.GetBytes(str, 0, maxlength, bytes, 0);
                        // Возможно, надо еще разметить остаток массива. Пока считаю, что имеется разметка нулями и этого достаточно
                        fs.Write(bytes, 0, bytes.Length);
                    }
                    break;
                case PTypeEnumeration.sstring:
                    {
                        string str = (string)value;
                        //bw.Write(str.Length);
                        //byte[] info = new UTF8Encoding(true).GetBytes(str);
                        //fs.Write(info, 0, info.Length);
                        bw.Write(str);
                    }
                    break;
                case PTypeEnumeration.record:
                    {
                        PTypeRecord mtr = (PTypeRecord)typ;
                        int ind_value = 0;
                        foreach (var ft in mtr.Fields)
                        {
                            PType tel = ft.Type;
                            object[] els = (object[])value;
                            Append(tel, els[ind_value]);
                            ind_value++;
                        }
                    }
                    break;
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        object[] els = (object[])value;
                        // Внешний уровень определяем по позиции указателя
                        if (this.fs.Position == this.dataStart)
                        {
                            this.nElements = els.Length;
                        }
                        bw.Write((long)els.Length);
                        foreach (var el in els) { Append(tel, el); }
                    }
                    break;
                case PTypeEnumeration.union:
                    {
                        PTypeUnion tpu = (PTypeUnion)typ;
                        object[] pair = (object[])value;
                        int tag = (int)pair[0];
                        object val = pair[1];
                        bw.Write((byte)tag);
                        Append(tpu.Variants[tag].Type, val);
                    }
                    break;
                default:
                    throw new Exception("VStore.Append 3 exception");
            }
        }
        internal object ScanObject(PType typ)
        {
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: return null;
                case PTypeEnumeration.boolean: return br.ReadByte();
                case PTypeEnumeration.integer: return br.ReadInt32();
                case PTypeEnumeration.longinteger: return br.ReadInt64();
                case PTypeEnumeration.real: return br.ReadDouble();
                case PTypeEnumeration.@byte: return br.ReadByte();
                case PTypeEnumeration.sstring:
                    {
                        //int len = br.ReadInt32();
                        //char[] chrs = br.ReadChars(len);
                        //return new string(chrs);
                        return br.ReadString();
                    }
                case PTypeEnumeration.record:
                    {
                        PTypeRecord r_tp = (PTypeRecord)typ;
                        object[] fields = new object[r_tp.Fields.Length];
                        for (int i = 0; i < r_tp.Fields.Length; i++)
                        {
                            fields[i] = ScanObject(r_tp.Fields[i].Type);
                        }
                        return fields;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        long llen = br.ReadInt64();
                        object[] els = new object[llen];
                        for (long ii = 0; ii < llen; ii++) els[ii] = ScanObject(tel);
                        return els;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion mtu = (PTypeUnion)typ;
                        int v = br.ReadByte();
                        PType mt = mtu.Variants[v].Type;
                        return new object[] { v, ScanObject(mt) };
                    }

                default: throw new Exception("Err in TPath ScanObject(): type is not implemented " + typ.Vid);
            }
        }
    }
}
