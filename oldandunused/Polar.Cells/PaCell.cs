using System;
using System.Collections.Generic;
using System.Linq;
using Polar.DB;

namespace Polar.Cells
{
    public class PaCell : PCell
    {
        // Статус
        public bool activated = false;
        // Формирование кеша
        public virtual void ActivateCache()
        {
            throw new Exception("Depricated 20170217");
        }

        /// <summary>
        /// Новый вариан конструктора - на основе потока
        /// </summary>
        /// <param name="typ"></param>
        /// <param name="stream"></param>
        /// <param name="readOnly"></param>
        public PaCell(PType typ, System.IO.Stream stream, bool readOnly = true) : base(typ, false, stream, readOnly)
        {
            rt = new PaEntry(typ, this.dataStart, this);
        }

        public PaCell(PType typ, string filePath, bool readOnly = true)
            : base(typ, false, filePath, readOnly)
        {
            rt = new PaEntry(typ, this.dataStart, this);
        }
        private PaEntry rt;// = PaEntry2.Empty;
        public PaEntry Root
        {
            get
            {
                //    if (this.IsEmpty) throw new Exception("Root of empty PaCell is undefined");
                //    rt = new PaEntry2(this.Type, this.dataStart, this);
                rt.offset = this.dataStart; // Это на всякий случай, поскольку можно "испортить" Root
                return rt;
            }
        }
        // Эти процедуры не понадобились:
        //public void ResetForFreespace() { this.SetOffset(this.freespace); }
        //public void SetFreespace() { this.freespace = this.fs.Position; }

        public void Fill(object valu)
        {
            if (!this.IsEmpty) throw new Exception("PaCell is not empty");
            this.Restart();
            this.Append(this.Type, valu);
            this.freespace = this.fs.Position; // Это нужно для операции AppendElement
            this.Flush();
        }

        // ============ Чтения данных ============
        //public long NElements { get { return this.nElements; } }
        public string ReadString(long off, out long offout)
        {
            this.SetOffset(off);
            //int len = this.br.ReadInt32();
            //char[] chrs = this.br.ReadChars(len);
            //offout = this.GetOffset();
            //return new string(chrs);
            string s = br.ReadString();
            offout = this.GetOffset();
            return s;
        }
        public long ReadLong(long off)
        {
            this.SetOffset(off);
            long l = this.br.ReadInt64();
            return l;
        }
        public int ReadByte(long off)
        {
            this.SetOffset(off);
            int v = this.br.ReadByte();
            return v;
        }
        public long ReadCount(long off)
        {
            // Для внешних последовательностей длину берем из объекта cell
            if (off == this.Root.offset) return this.nElements;
            // Для остальных - двойное целое вначале
            return ReadLong(off);
        }
        public object GetPObject(PType typ, long off, out long offout)
        {
            //if (toflush) Flush();
            this.SetOffset(off);
            object v = GetPO(typ, this.br);
            offout = this.GetOffset();
            return v;
        }
        public object GetPObject(PType typ, long off)
        {
            this.SetOffset(off);
            object v = GetPO(typ, this.br);
            return v;
        }
        /// <summary>
        /// Читает P-объект из бинарного ридера, начиная с текущего места
        /// </summary>
        /// <param name="typ"></param>
        /// <param name="br"></param>
        /// <returns></returns>
        public static object GetPO(PType typ, System.IO.BinaryReader br)
        {
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: return null;
                case PTypeEnumeration.boolean: return br.ReadBoolean();
                case PTypeEnumeration.integer: return br.ReadInt32();
                case PTypeEnumeration.longinteger: return br.ReadInt64();
                case PTypeEnumeration.real: return br.ReadDouble();
                case PTypeEnumeration.@byte: return br.ReadByte();
                case PTypeEnumeration.fstring:
                    {
                        //int len = ((PTypeFString)typ).Length;
                        int size = ((PTypeFString)typ).Size;
                        byte[] arr = new byte[size];
                        arr = br.ReadBytes(size);
                        string s = System.Text.Encoding.Unicode.GetString(arr);
                        return s;
                    }
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
                            fields[i] = GetPO(r_tp.Fields[i].Type, br);
                        }
                        return fields;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        long llen = br.ReadInt64();
                        object[] els = new object[llen];
                        for (long ii = 0; ii < llen; ii++) els[ii] = GetPO(tel, br);
                        return els;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion mtu = (PTypeUnion)typ;
                        int v = br.ReadByte();
                        PType mt = mtu.Variants[v].Type;
                        return new object[] { v, GetPO(mt, br) };
                    }

                default: throw new Exception("Err in TPath Get(): type is not implemented " + typ.Vid);
            }
        }
        internal void SetPObject(PType tp, long off, object valu)
        {
            if (!tp.HasNoTail) throw new Exception("Set can't be implemented to HasTail type");
            this.SetOffset(off);
            this.Append(tp, valu);
        }
        internal long AppendPObj(PType tp, object valu)
        {
            //long off = this.freespace;
            //this.SetOffset(off);
            //this.Append(tp, valu);
            //if (tp.HasNoTail)
            //{
            //    this.freespace += tp.HeadSize;
            //}
            //else
            //{
            //    this.freespace = this.fs.Position;
            //}
            long off = this.fs.Position;
            PaCell.SetPO(tp, bw, valu);
            this.nElements += 1;
            this.freespace = tp.HasNoTail ? freespace + tp.HeadSize : fs.Position;
            return off;
        }
        /// <summary>
        /// Добавляет в поток бинарного райтера байтовую сериализацию объектного представления значения
        /// типа tp
        /// </summary>
        /// <param name="typ"></param>
        /// <param name="bw"></param>
        /// <param name="valu"></param>
        public static void SetPO(PType typ, System.IO.BinaryWriter bw, object valu)
        {
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: return;
                case PTypeEnumeration.boolean: bw.Write((bool)valu); return;
                case PTypeEnumeration.integer: bw.Write((int)valu); return;
                case PTypeEnumeration.longinteger: bw.Write((long)valu); return;
                case PTypeEnumeration.real: bw.Write((double)valu); return;
                case PTypeEnumeration.@byte: bw.Write((byte)valu); return;
                case PTypeEnumeration.fstring:
                    {
                        throw new Exception("Unimplemented variant fstring in PaCell.SetPObject()");
                    }
                case PTypeEnumeration.sstring:
                    {
                        string str = (string)valu;
                        //bw.Write(str.Length);
                        //bw.Write(str.ToCharArray());
                        bw.Write(str);
                        return;
                    }
                case PTypeEnumeration.record:
                    {
                        PTypeRecord r_tp = (PTypeRecord)typ;
                        object[] fields = (object[])valu;
                        for (int i = 0; i < r_tp.Fields.Length; i++)
                        {
                            SetPO(r_tp.Fields[i].Type, bw, fields[i]);
                        }
                        return;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        object[] els = (object[])valu;
                        long llen = els.Length;
                        bw.Write(llen);
                        for (long ii = 0; ii < llen; ii++) SetPO(tel, bw, els[ii]);
                        return;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion mtu = (PTypeUnion)typ;
                        object[] pair = (object[])valu;
                        int v = (int)pair[0];
                        bw.Write((byte)v);
                        PType mt = mtu.Variants[v].Type;
                        SetPO(mt, bw, pair[1]); return;
                    }

                default: throw new Exception("Err in TPath SetPO(): type is not implemented " + typ.Vid);
            }
        }

        //================== Сортировки ====================
        /// <summary>
        /// Слияние двух участков одной ячейки первого (он в младшей индексной части) и второго, следующего за ним.
        /// Результирующий массив начинается с начала первого участка. Слияние производится сравнением объектных значений
        /// элементов посредством функции сравнения ComparePO
        /// </summary>
        /// <param name="tel">Тип элемента последовательности</param>
        /// <param name="off1">offset начала первого участка</param>
        /// <param name="number1">длина первого участка</param>
        /// <param name="off2">offset начала второго участка</param>
        /// <param name="number2">длина второго участка</param>
        /// <param name="comparePO">Функция сравнения объектных представлений элементов</param>
        internal void CombineParts(PType tel, long off1, long number1, long off2, long number2, Func<object, object, int> comparePO)
        {
            long pointer_out = off1;
            long pointer_in = off2;
            int size = tel.HeadSize;

            // Используем временный файл
            string tmp_fname = "tmp_merge.pac";
            System.IO.FileStream tmp_fs = new System.IO.FileStream(tmp_fname, System.IO.FileMode.Create, System.IO.FileAccess.ReadWrite);
            System.IO.BinaryReader tmp_br = new System.IO.BinaryReader(tmp_fs);

            // Перепишем number1 * size байтов от начальной точки во временный файл (Первую часть)
            this.SetOffset(pointer_out);
            long bytestocopy = number1 * size;
            int buffLen = 8192;
            byte[] buff = new byte[buffLen * size];
            while (bytestocopy > 0)
            {
                int nb = bytestocopy < buff.Length ? (int)bytestocopy : buff.Length;
                this.fs.Read(buff, 0, nb);
                tmp_fs.Write(buff, 0, nb);
                bytestocopy -= nb;
            }
            tmp_fs.Flush();

            // теперь будем сливать массивы
            long cnt1 = 0; // число переписанных элементов первой подпоследовательности
            long cnt2 = 0; // число переписанных элементов второй подпоследовательности
            tmp_fs.Position = 0L; // Установка начальной позиции в файле
            this.SetOffset(pointer_in); // Установка на начало второй части последовательности в ячейке

            System.IO.Stream fs1 = tmp_fs; // Первый поток
            System.IO.Stream fs2 = this.fs; // Второй поток

            // Микробуфера для элементов
            byte[] m_buf1 = new byte[size];
            byte[] m_buf2 = new byte[size];

            // Заполнение микробуферов
            fs1.Read(m_buf1, 0, size);
            fs2.Read(m_buf2, 0, size);
            pointer_in += size;
            // Это чтобы читать P-значения
            System.IO.BinaryReader m_reader1 = new System.IO.BinaryReader(new System.IO.MemoryStream(m_buf1));
            System.IO.BinaryReader m_reader2 = new System.IO.BinaryReader(new System.IO.MemoryStream(m_buf2));
            // Текущие объекты
            object val1 = PaCell.GetPO(tel, m_reader1);
            object val2 = PaCell.GetPO(tel, m_reader2);
            // Писать будем в тот же буффер buff, текущее место записи:
            int ind_buff = 0;

            // Слияние!!!
            while (cnt1 < number1 && cnt2 < number2)
            {
                if (comparePO(val1, val2) < 0)
                { // Продвигаем первую подпоследовательность
                    Buffer.BlockCopy(m_buf1, 0, buff, ind_buff, size);
                    cnt1++;
                    if (cnt1 < number1) // Возможен конец
                    {
                        fs1.Read(m_buf1, 0, size);
                        m_reader1.BaseStream.Position = 0;
                        val1 = PaCell.GetPO(tel, m_reader1);
                    }
                }
                else
                { // Продвигаем вторую последовательность
                    Buffer.BlockCopy(m_buf2, 0, buff, ind_buff, size);
                    cnt2++;
                    if (cnt2 < number2) // Возможен конец
                    {
                        this.SetOffset(pointer_in); // Установка на текущее место чтения !!!!!!!!!
                        fs2.Read(m_buf2, 0, size);
                        pointer_in += size;
                        m_reader2.BaseStream.Position = 0;
                        val2 = PaCell.GetPO(tel, m_reader2);
                    }
                }
                ind_buff += size;
                // Если буфер заполнился, его надо сбросить
                if (ind_buff == buff.Length)
                {
                    this.SetOffset(pointer_out);
                    int volume = ind_buff;
                    this.fs.Write(buff, 0, volume);
                    pointer_out += volume;
                    ind_buff = 0;
                }
            }
            // Если в буфере остались данные, их надо сбросить
            if (ind_buff > 0)
            {
                this.SetOffset(pointer_out);
                int volume = ind_buff;
                this.fs.Write(buff, 0, volume);
                pointer_out += volume;
            }
            // Теперь надо переписать остатки
            if (cnt1 < number1)
            {
                this.SetOffset(pointer_out); // Здесь запись ведется подряд, без перемещений головки чтения/записи
                // Сначала запишем уже прочитанную запись
                this.fs.Write(m_buf1, 0, size);
                // а теперь - остальное
                bytestocopy = (number1 - cnt1 - 1) * size;
                while (bytestocopy > 0)
                {
                    int nbytes = buff.Length;
                    if (bytestocopy < nbytes) nbytes = (int)bytestocopy;
                    fs1.Read(buff, 0, nbytes);
                    this.fs.Write(buff, 0, nbytes);
                    bytestocopy -= nbytes;
                }
            }
            else if (cnt2 < number2)
            { // Поскольку в тот же массив, то надо переписать только прочитанный, а остатки массива уже на своем месте
                //this.cell.SetOffset(pointer_out);
                //this.cell.fs.Write(m_buf2, 0, size);
            }
            this.fs.Flush();
            // Закрываем временный файл
            tmp_fs.Flush();
            tmp_fs.Dispose();
            // Убираем временный файл
            System.IO.File.Delete(tmp_fname); // не убираем, а то он помещается в мусорную корзину, а так - будет переиспользоваться
        }

    }
}
