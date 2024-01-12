using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Cells
{
    public struct PxEntry
    {
        public long offset;
        public bool IsEmpty { get { return offset == long.MinValue; } }
        private PType typ;
        public PType Typ { get { return this.typ; } }
        public PxCell fis; //TODO: Надо защитить это поле
        public PxEntry(PType tp, long offset, PxCell fis)
        {
            this.typ = tp;
            this.offset = offset;
            this.fis = fis;
        }
        // ========== Для записей =========
        public PxEntry Field(int ind)
        {
            if (this.typ.Vid != PTypeEnumeration.record) throw new Exception("Err: Field() need record");
            PTypeRecord mtr = (PTypeRecord)this.typ;
            if (ind < 0 || ind >= mtr.Fields.Length) throw new Exception("Wrong index in Field()");
            var shift = mtr.Fields.Take(ind).Select(pair => pair.Type.HeadSize).Sum(); // .Where((f, i) => i < ind)
            return new PxEntry(
                mtr.Fields[ind].Type,
                this.offset + shift,
                this.fis);
        }
        // ============= Для последовательностей =============
        public long Count()
        {
            if (this.typ.Vid != PTypeEnumeration.sequence) throw new Exception("Err: Count() need sequense");
            // Для внешних последовательностей длину берем из объекта cell
            if (this.offset == this.fis.Root.offset) return this.fis.nElements;
            fis.SetOffset(offset);
            return fis.br.ReadInt64();
        }
        public PxEntry Element(long ind)
        {
            if (this.typ.Vid != PTypeEnumeration.sequence) throw new Exception("Err: Element(long) need sequense");
            // Устаревшая версия
            fis.SetOffset(offset);
            long nc = fis.br.ReadInt64();
            if (ind < 0 || ind >= nc) throw new Exception("Err: index out of bound");
            fis.br.ReadInt64();
            PType tel = ((PTypeSequence)this.typ).ElementType;
            long off = fis.br.ReadInt64() + ind * tel.HeadSize;
            // Неудачная (!!!) попытка оптимизации
            //if (ind < 0 || ind >= this.Count()) throw new Exception("Err: index out of bound");
            //PType tel = ((PTypeSequence)this.typ).ElementType;
            //long off = this.offset + ind * tel.HeadSize;

            return new PxEntry(tel, off, fis);
        }
        public IEnumerable<PxEntry> Elements()
        {
            if (this.typ.Vid != PTypeEnumeration.sequence) throw new Exception("Err: Elements() need sequense");
            long llen = this.Count();
            if (llen != 0)
            {
                fis.SetOffset(offset + 8);
                fis.br.ReadInt64();
                PType tel = ((PTypeSequence)this.typ).ElementType;
                int size = tel.HeadSize;
                long off = fis.br.ReadInt64();
                for (long ii = 0; ii < llen; ii++)
                {
                    yield return new PxEntry(tel, off, fis);
                    off += size;
                }
            }
        }
        // ============= Для объединений =============
        public int Tag()
        {
            if (this.typ.Vid != PTypeEnumeration.union) throw new Exception("Err: Tag() need union");
            fis.SetOffset(offset);
            return fis.br.ReadByte();
        }
        public PxEntry UElement()
        {
            if (this.typ.Vid != PTypeEnumeration.union) throw new Exception("Err: Element() need union");
            fis.SetOffset(offset);
            int tag = fis.br.ReadByte();
            PTypeUnion ptu = ((PTypeUnion)this.typ);
            if (tag < 0 || tag >= ptu.Variants.Length) throw new Exception("Err: tag is out of bound");
            PType tel = ptu.Variants[tag].Type;
            long off;
            if (tel.IsAtom) off = offset + 1;
            else            off = fis.br.ReadInt64();
            return new PxEntry(tel, off, fis);
        }
        public PxEntry UElementUnchecked(int tag)
        {
            PTypeUnion ptu = ((PTypeUnion)this.typ);
            if (tag < 0 || tag >= ptu.Variants.Length) throw new Exception("Err: tag is out of bound");
            PType tel = ptu.Variants[tag].Type;
            long off;
            if (tel.IsAtom) off = offset + 1;
            else
            {
                fis.SetOffset(offset + 1);
                off = fis.br.ReadInt64();
            }
            return new PxEntry(tel, off, fis);
        }
        // В данный вход объединения записать указатель на уже сформированный вход подэлемента 
        //TODO: процедура техническая, надо ее сделать внутренней
        //public void SetElement(PxEntry pointer)
        //{
        //    if (this.typ.Vid != PTypeEnumeration.union) throw new Exception("Err: SetElement() need union");
        //    PTypeUnion ptu = (PTypeUnion)this.typ;
        //    fis.SetOffset(this.offset);
        //    int tag = fis.br.ReadByte();
        //    if (tag < 0 || tag >= ptu.variants.Length) throw new Exception("wrong tag for SetElement()");
        //    PType tel = ptu.variants[tag].Type;
        //    //TODO: Частичная проверка на соответствие типов
        //    if (pointer.Typ.Vid != tel.Vid) throw new Exception("wrong element type for SetElement()");
        //    fis.bw.Write(pointer.offset);
        //}

        // ============= Взять значение =============
        public object Get()
        {
            return GetPObject(this.typ, this.offset, this.fis);
        }
        public PValue GetValue()
        {
            object obj = GetPObject(this.typ, this.offset, this.fis);
            return new PValue(this.typ, this.offset, obj);
        }
        public void Set(object valu)
        {
            // Возможно, стоит убрать следующую установку и внести ее в варианты, этого требующие
            fis.SetOffset(this.offset);
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: break;
                case PTypeEnumeration.boolean: fis.bw.Write((bool)valu); break;
                case PTypeEnumeration.character:
                    {
                        char ch = (char)valu;
                        var cc = ch - '\0';
                        //char.ConvertToUtf32(
                        fis.bw.Write((ushort)cc);
                        break;
                    }
                case PTypeEnumeration.integer: fis.bw.Write((int)valu); break;
                case PTypeEnumeration.longinteger: fis.bw.Write((long)valu); break;
                case PTypeEnumeration.real: fis.bw.Write((double)valu); break;
                case PTypeEnumeration.@byte: fis.bw.Write((byte)valu); break;
                case PTypeEnumeration.fstring:
                    {
                        string s = (string)valu;
                        int size = ((PTypeFString)typ).Size;
                        byte[] arr = new byte[size];
                        if (s.Length * 2 > size) s = s.Substring(0, size / 2);
                        var qu = System.Text.Encoding.Unicode.GetBytes(s, 0, s.Length, arr, 0);
                        fis.bw.Write(arr);
                    }
                    break;
                case PTypeEnumeration.sstring:
                    {
                        string s = (string)valu;
                        int len = s.Length;
                        fis.bw.Write(len);
                        if (len > 0)
                        { 
                            long off = this.fis.freespace;
                            this.fis.freespace += 2 * len;
                            fis.bw.Write(off);
                            //vtoList.Enqueue(new VTO(valu, typ, off)); // Вместо "метания" записи, стоит использовать очередь или что-то подбное
                            byte[] bytes = Encoding.Unicode.GetBytes(s);
                            if (bytes.Length != s.Length * 2) throw new Exception("Assert Error in Set(string)");
                            fis.SetOffset(off);
                            fis.bw.Write(bytes);
                        }
                    }
                    break;
                case PTypeEnumeration.record:
                    {
                        PTypeRecord mtr = (PTypeRecord)typ;
                        object[] arr = (object[])valu;
                        if (arr.Length != mtr.Fields.Length) throw new Exception("Err in Set(): record has wrong number of fields");
                        long field_offset = this.offset;
                        for (int i = 0; i < arr.Length; i++)
                        {
                            PxEntry entry = new PxEntry(mtr.Fields[i].Type, field_offset, this.fis);
                            //FillHead(arr[i], mtr.Fields[i].Type, vtoList);
                            entry.Set(arr[i]);
                            //if (i < arr.Length - 1) // Добавление не мешает, можно не проверять
                                field_offset += mtr.Fields[i].Type.HeadSize;
                        }
                    }
                    break;
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        // Пишем голову последовательности
                        object[] arr = (object[])valu;
                        long llen = arr.Length;

                        // Внешний уровень определяем по позиции указателя
                        if (this.offset == fis.dataStart)
                        {
                            fis.nElements = llen;
                        }

                        fis.bw.Write(llen);
                        // Следующие три оператора можно не делать для пустых последовательносте, но даст ли это экономию, неизвестно
                        long off = fis.freespace;
                        fis.bw.Write(0L);
                        fis.bw.Write(off);
                            // Если есть хвост, ставим в очередь
                            //if (llen > 0) vtoList.Enqueue(new VTO(valu, typ, off));
                        // Если есть элементы, заведем место, сформируем массив и запишем элементы
                        if (llen > 0)
                        {
                            int size = tel.HeadSize;
                            fis.freespace += size * llen;
                            PxEntry entry = new PxEntry(tel, off, fis);
                            for (long i = 0; i < llen; i++)
                            {
                                entry.Set(arr[i]);
                                entry.offset += size;
                            }
                        }
                    }
                    break;
                case PTypeEnumeration.union:
                    {
                        object[] arr = (object[])valu;
                        int tag = (int)arr[0];
                        PTypeUnion mtu = (PTypeUnion)typ;
                        PType tel = mtu.Variants[tag].Type;
                        // Пишем голову
                        fis.bw.Write((byte)(int)arr[0]);
                        if (tel.IsAtom)
                        {
                            if (arr[1] == null) fis.bw.Write(-1L);
                            else
                            {
                                fis.WriteAtomAsLong(tel.Vid, arr[1]);
                            }
                        }
                        else
                        {
                            long off = fis.freespace;
                            fis.freespace += tel.HeadSize;
                            fis.bw.Write(off);
                            //vtoList.Enqueue(new VTO(arr[1], tel, off) { makehead = true });
                            PxEntry subelement = new PxEntry(tel, off, fis);
                            subelement.Set(arr[1]);
                        }
                    }
                    break;
                default: throw new Exception("unexpected type");
            }
        }
        // ============= Записать значение =============
        // TODO: Возможно, не работает, но надо вернуться к идее и реализовать ее качественно
        //public void Set(object valu)
        //{
        //    fis.fs.Position = this.offset;
        //    Queue<VTO> vtoList = new Queue<VTO>();
        //    vtoList.Enqueue(new VTO(valu, this.typ, this.offset) { makehead = true });
        //    while (vtoList.Count > 0)
        //    {
        //        var listElement = vtoList.Dequeue();
        //        if (listElement.makehead) 
        //        {
        //            fis.SetOffset(listElement.off);
        //            fis.FillHead(listElement.value, listElement.ty, vtoList);
        //        }
        //        else
        //        {
        //            fis.FillTail(listElement.value, listElement.ty, vtoList);
        //        }
        //    }
        //}
        /// <summary>
        /// Размечает последовательность "пустыми" (нулевыми) элементами в количестве length 
        /// </summary>
        /// <param name="length"></param>
        public void SetRepeat(long length)
        {
            if (typ.Vid != PTypeEnumeration.sequence) throw new Exception("SetRepeat is defined only for sequences");
            if (length < 0) throw new Exception("Negative repeat length"); 
            PTypeSequence mts = (PTypeSequence)typ;
            PType tel = mts.ElementType;
            // Пишем голову последовательности
            long llen = length;
            long off = this.fis.freespace;

            // Внешний уровень определяем по позиции указателя
            if (this.offset == this.fis.dataStart)
            {
                this.fis.nElements = llen;
            }

            this.fis.SetOffset(this.offset);
            this.fis.bw.Write(llen);
            this.fis.bw.Write(0L);
            this.fis.bw.Write(off);
            this.fis.freespace += tel.HeadSize * llen;
            // Заполним нулями
            this.fis.SetOffset(off);
            byte b = 0;
            for (long i = 0; i < tel.HeadSize * llen; i++) this.fis.bw.Write(b);
        }
        // ================= Методы специального назначения =================
        public byte[] GetHead()
        {
            byte[] arr = new byte[Typ.HeadSize];
            fis.SetOffset(this.offset);
            fis.br.Read(arr, 0, Typ.HeadSize);
            return arr;
        }
        public void SetHead(byte[] arr)
        {
            if (arr.Length != Typ.HeadSize) throw new Exception("Err in GetHead - arr is of different size");
            fis.SetOffset(this.offset);
            fis.bw.Write(arr, 0, Typ.HeadSize);
        }
        /*
        private void ReplaceHead(object valu)
        {
            fis.SetOffset(this.offset);
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: break;
                case PTypeEnumeration.boolean: fis.bw.Write((bool)valu);
                case PTypeEnumeration.character: fis.bw.Write((short)valu);
                case PTypeEnumeration.integer: fis.bw.Write((int)valu);
                case PTypeEnumeration.longinteger: fis.bw.Write((long)valu);
                case PTypeEnumeration.real: fis.bw.Write((double)valu);
                case PTypeEnumeration.sstring:
                    {
                        int len = fis.br.ReadInt32();
                        long off = fis.br.ReadInt64();
                        if (len > 0)
                        {
                            fis.SetOffset(off);
                            byte[] b = fis.br.ReadBytes(len * 2);
                            return Encoding.Unicode.GetString(b);
                        }
                        else return "";
                    }
                case PTypeEnumeration.record:
                    {
                        PTypeRecord r_tp = (PTypeRecord)typ;
                        object[] fields = new object[r_tp.Fields.Length];
                        long off = offse;
                        for (int i = 0; i < r_tp.Fields.Length; i++)
                        {
                            PType ftyp = r_tp.Fields[i].Type;
                            fields[i] = GetPObject(ftyp, off, fis);
                            off += ftyp.HeadSize;
                        }
                        return fields;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        long llen = fis.br.ReadInt64();
                        fis.br.ReadInt64();
                        long off = fis.br.ReadInt64();
                        object[] els = new object[llen];
                        for (long ii = 0; ii < llen; ii++)
                        {
                            els[ii] = GetPObject(tel, off, fis);
                            off += tel.HeadSize;
                        }
                        return els;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion mtu = (PTypeUnion)typ;
                        int v = fis.br.ReadByte();
                        long off = fis.br.ReadInt64();
                        PType mt = mtu.Variants[v].Type;
                        if (mt.IsAtom) return new object[] { v, GetPObject(mt, offse + 1, fis) };
                        else return new object[] { v, GetPObject(mt, off, fis) };
                    }

                default: throw new Exception("Err in TPath Get(): type is not implemented " + typ.Vid);
            }
        }
         */
        private static object GetPObject(PType typ, long offse, PxCell fis)
        {
            fis.SetOffset(offse);
            switch (typ.Vid)
            {
                case PTypeEnumeration.none: return null;
                case PTypeEnumeration.boolean: return fis.br.ReadBoolean();
                case PTypeEnumeration.character: return fis.br.ReadChar();
                case PTypeEnumeration.integer: return fis.br.ReadInt32();
                case PTypeEnumeration.longinteger: return fis.br.ReadInt64();
                case PTypeEnumeration.real: return fis.br.ReadDouble();
                case PTypeEnumeration.@byte: return fis.br.ReadByte();
                case PTypeEnumeration.sstring:
                    {
                        int len = fis.br.ReadInt32();
                        long off = fis.br.ReadInt64();
                        if (len > 0)
                        {
                            fis.SetOffset(off);
                            byte[] b = fis.br.ReadBytes(len * 2);
                            return Encoding.Unicode.GetString(b);
                        }
                        else return "";
                    }
                case PTypeEnumeration.fstring:
                    {
                        int size = ((PTypeFString)typ).Size;
                        byte[] arr = new byte[size];
                        arr =  fis.br.ReadBytes(size);
                        int count = size / 2;
                        while (count > 0 && arr[2 * count - 1] == 0 && arr[2 * count - 2] == 0) count--; 
                        string s = System.Text.Encoding.Unicode.GetString(arr, 0, 2 * count);
                        return s;
                    }
                case PTypeEnumeration.record:
                    {
                        PTypeRecord r_tp = (PTypeRecord)typ;
                        object[] fields = new object[r_tp.Fields.Length];
                        long off = offse;
                        for (int i = 0; i < r_tp.Fields.Length; i++)
                        {
                            PType ftyp = r_tp.Fields[i].Type;
                            fields[i] = GetPObject(ftyp, off, fis);
                            off += ftyp.HeadSize;
                        }
                        return fields;
                    }
                case PTypeEnumeration.sequence:
                    {
                        PTypeSequence mts = (PTypeSequence)typ;
                        PType tel = mts.ElementType;
                        long llen = fis.br.ReadInt64();
                        fis.br.ReadInt64();
                        long off = fis.br.ReadInt64();
                        object[] els = new object[llen];
                        for (long ii = 0; ii < llen; ii++)
                        {
                            els[ii] = GetPObject(tel, off, fis);
                            off += tel.HeadSize;
                        }
                        return els;
                    }
                case PTypeEnumeration.union:
                    {
                        PTypeUnion mtu = (PTypeUnion)typ;
                        int v = fis.br.ReadByte();
                        PType mt = mtu.Variants[v].Type;
                        long off = fis.br.ReadInt64();
                        if (mt.IsAtom) 
                            return new object[] { v, off == -1L 
                                                    ? null
                                                    : fis.ReadAtomFromLong(mt.Vid, offse + 1) };                            
                        else
                        {
                            return new object[] { v, GetPObject(mt, off, fis) };
                        }
                    }

                default: throw new Exception("Err in TPath Get(): type is not implemented " + typ.Vid);
            }
        }
        
        /// <summary>
        /// Метод реализует сортировку, т.е. перестановку элементов последовательности в соотвествии с функцией
        /// сравнения, заданной аргументом
        /// </summary>
        /// <param name="compare">Функция, задающая сравнение друх входов</param>
        public void SortComparison(System.Comparison<PxEntry> compare)
        {
            if (typ.Vid != PTypeEnumeration.sequence) throw new Exception("SortComparison can't be implemented to this vid");
            PTypeSequence pts = (PTypeSequence)typ;
            long llen = this.Count();
            if (llen < 2) return; // сортировать не нужно
            // Указатель на нулевой элемент и размер головы записи
            long p0 = this.Element(0).offset;
            int size = pts.ElementType.HeadSize;
            // организуем массив offset'ов - указателей на головы элементов
            long[] offsets = new long[llen];
            for (long ind = 0; ind < llen; ind++) offsets[ind] = p0 + size * ind;
            //теперь сортируем используя функцию сравнения
            PxEntry e1 = new PxEntry(pts.ElementType, long.MinValue, this.fis);
            PxEntry e2 = new PxEntry(pts.ElementType, long.MinValue, this.fis);
            Array.Sort<long>(offsets, (long o1, long o2) =>
            {
                e1.offset = o1;
                e2.offset = o2;
                return compare(e1, e2);
            });
            // Надеюсь, отсортировали

            // Превращаю массив смещений в массив индексов элементов
            int[] indexes = new int[llen];
            for (long ind = 0; ind < llen; ind++) indexes[ind] = (int)((offsets[ind] - p0) / size);
            // теперь в i-ом элементе массива находится индекс элемента (головы), который должен попасть на i-ю позицию
            ReorderSequenceArrayHeads(llen, p0, size, indexes);
        }

        private void ReorderSequenceArrayHeads(long llen, long p0, int size, int[] indexes)
        {
            // Следующим шагом надо сделать два массива байтов для голов один входной, другой выходной
            byte[] headsin = new byte[llen * size];
            byte[] headsout = new byte[llen * size];
            // Надо все головы переписать во входной массив
            this.fis.SetOffset(p0);
            this.fis.fs.Read(headsin, 0, (int)(llen * size)); //TODO: это надо переделывать когда будут произвольные длинные размеры  
            // 
            for (long ind = 0; ind < llen; ind++)
            {
                Array.Copy(headsin, indexes[ind] * size, headsout, (int)(ind * size), size);
            }
            // Заключительное действие - переписать выходной массив вместо всех голов
            this.fis.SetOffset(p0);
            this.fis.fs.Write(headsout, 0, (int)(llen * size)); //TODO: это надо переделывать когда будут произвольные длинные размеры  
        }
        public void Sort(KeyString keyfunction)
        {
            if (typ.Vid != PTypeEnumeration.sequence) throw new Exception("Sort can't be implemented to this vid");
            PTypeSequence pts = (PTypeSequence)typ;
            long llen = this.Count();
            if (llen < 2) return; // сортировать не нужно
            // Указатель на нулевой элемент и размер головы записи
            long p0 = this.Element(0).offset;
            int size = pts.ElementType.HeadSize;
            // организуем массивы ключей и индексов - номеров записей
            string[] keys = new string[llen];
            int[] indexes = Enumerable.Range(0, (int)llen).ToArray();
            // Вычислим и запишем ключи
            int i = 0;
            foreach (PxEntry e in this.Elements())
            {
                keys[i] = keyfunction(e);
                i++;
            }
            // Сортируем два массива
            Array.Sort(keys, indexes);
            ReorderSequenceArrayHeads(llen, p0, size, indexes);
        }
        //
        private void Sort<T>(long start, long number, Func<PxEntry, T> keyfunction)
        {
            PTypeSequence pts = (PTypeSequence)typ;
            if (number < 2) return; // сортировать не нужно
            if (number < 10000000)
            {
                // Указатель на начальный элемент и размер головы записи
                PxEntry e = this.Element(start);
                long p0 = e.offset;
                int size = pts.ElementType.HeadSize;
                // организуем массивы ключей и индексов - номеров записей
                T[] keys = new T[number];
                int[] indexes = Enumerable.Range(0, (int)number).ToArray();
                // Вычислим и запишем ключи

                for (long ii = 0; ii < number; ii++)
                {
                    keys[ii] = keyfunction(e);
                    e.offset += size;
                }
                // Сортируем два массива
                Array.Sort(keys, indexes);
                ReorderSequenceArrayHeads(number, p0, size, indexes);
            }
            else
            {
            }
        }
        private void MergeUp<T>(long start, long number1, long number2, Func<PxEntry, T> keyfunction)
        {
            if (number1 == 0 || number2 == 0) return;
            PTypeSequence pts = (PTypeSequence)typ;
            int size = pts.ElementType.HeadSize;
            string tmp_name = "../../../Databases/temp.bin";
            System.IO.FileStream tmp = new System.IO.FileStream(tmp_name, System.IO.FileMode.Create, System.IO.FileAccess.ReadWrite, System.IO.FileShare.None, 10000);
            // Cкопируем нужное число байтов
            int buff_size = 8192;
            byte[] buffer = new byte[buff_size];
            PxEntry entry1 = this.Element(start);
            long pointer = entry1.offset;
            this.fis.SetOffset(pointer);
            long bytestocopy = number1 * size;
            while (bytestocopy > 0)
            {
                int nb = this.fis.fs.Read(buffer, 0, bytestocopy < buff_size ? (int)bytestocopy : buff_size);
                tmp.Write(buffer, 0, nb);
                bytestocopy -= nb;
            }
            // Вычислим и запомним во временной ячейке ключи массива 1.
            // сначала тип ячейки и ячейка:
            PxEntry entry2 = this.Element(start + number1); // Это понадобится в будущем, а пока используем для определения типа ключа
            IComparable key2 = (IComparable)keyfunction(entry2);
            PType tp_key = key2 is string ? new PType(PTypeEnumeration.sstring) :
                (key2 is int ? new PType(PTypeEnumeration.integer) :
                (key2 is long ? new PType(PTypeEnumeration.longinteger) :
                (key2 is byte ? new PType(PTypeEnumeration.@byte) :
                (key2 is double ? new PType(PTypeEnumeration.real) : null))));

            PType tp_tmp = new PTypeSequence(tp_key);
            string tmp_cell_name = "../../../Databases/tmp.pac"; //TODO: Надо имя файла генерировать, а то могут быть коллизии
            PaCell tmp_cell = new PaCell(tp_tmp, tmp_cell_name, false);
            // теперь заполним ее значениями
            //tmp_cell.StartSerialFlow();
            //tmp_cell.S();
            //for (long ii = 0; ii < number1; ii++)
            //{
            //    var key = keyfunction(entry1);
            //    tmp_cell.V(key);
            //    entry1.offset += size;
            //}
            //tmp_cell.Se();
            //tmp_cell.EndSerialFlow();
            tmp_cell.Fill(new object[0]);
            for (long ii = 0; ii < number1; ii++)
            {
                var key = keyfunction(entry1);
                tmp_cell.Root.AppendElement(key);
                entry1.offset += size;
            }
            tmp_cell.Flush();
            // теперь будем сливать массивы
            tmp.Position = 0L; // в файле tmp
            long pointer2 = pointer + number1 * size;
            long tmp_offset = tmp_cell.Root.Element(0).offset;
            tmp_cell.SetOffset(tmp_offset);
            //PType tp_sstring = new PType(PTypeEnumeration.sstring);
            IComparable key1 = (IComparable)tmp_cell.ScanObject(tp_key);
            long cnt1 = 0; // число переписанных элементов первой подпоследовательности
            //PxEntry entry2 = this.Element(start + number1);
            //var key2 = keyfunction(entry2); // Уже прочитан
            long cnt2 = 0; // число переписанных элементов второй подпоследовательности
            byte[] buff = new byte[size * 8192];
            int buff_pos = 0;
            while (cnt1 == number1 || cnt2 == number2)
            {
                if (key1.CompareTo(key2) < 0)
                { // Продвигаем первую подпоследовательность
                    tmp.Read(buff, buff_pos, size); buff_pos += size;
                    cnt1++;
                    if (cnt1 < number1) key1 = (IComparable)tmp_cell.ScanObject(tp_key); // Возможен конец
                }
                else
                { // Продвигаем вторую последовательность
                    entry2.fis.SetOffset(pointer2);
                    pointer2 += size;
                    entry2.fis.fs.Read(buff, buff_pos, size); buff_pos += size;
                    cnt2++;
                    if (cnt2 < number2) key2 = (IComparable)keyfunction(entry2); // Возможен конец
                }
                // Если буфер заполнился, его надо сбросить
                if (buff_pos == buff.Length)
                {
                    this.fis.SetOffset(pointer);
                    this.fis.fs.Write(buff, 0, buff_pos);
                    pointer += buff_pos;
                    buff_pos = 0;
                }
            }
            // Теперь надо переписать остатки
            if (cnt1 < number1)
            {
                this.fis.SetOffset(pointer); // Здесь запись ведется подряд, без перемещений головки чтения/записи
                bytestocopy = (number1 - cnt1) * size;
                while (bytestocopy > 0)
                {
                    int nbytes = buff.Length - buff_pos;
                    if (bytestocopy < nbytes) nbytes = (int)bytestocopy;
                    tmp.Read(buff, buff_pos, nbytes); buff_pos += nbytes;
                    this.fis.fs.Write(buff, 0, buff_pos);
                    buff_pos = 0;
                }
            }
            else if (cnt2 < number2)
            {
                bytestocopy = (number2 - cnt2) * size;
                while (bytestocopy > 0)
                {
                    int nbytes = buff.Length - buff_pos;
                    if (bytestocopy < nbytes) nbytes = (int)bytestocopy;
                    this.fis.SetOffset(pointer2); //TODO: Здесь вроде я не то беру, надо разобраться (2 строчки ниже!)
                    pointer2 += nbytes;
                    tmp.Read(buff, buff_pos, nbytes); buff_pos += nbytes;
                    this.fis.SetOffset(pointer);
                    this.fis.fs.Write(buff, 0, buff_pos);
                    pointer += buff_pos;
                    buff_pos = 0;
                }
            }
        }
        private void Sort(long start, long number, KeyString keyfunction)
        {
            PTypeSequence pts = (PTypeSequence)typ;
            if (number < 2) return; // сортировать не нужно
            if (number < 10000000)
            {
                // Указатель на начальный элемент и размер головы записи
                PxEntry e = this.Element(start);
                long p0 = e.offset;
                int size = pts.ElementType.HeadSize;
                // организуем массивы ключей и индексов - номеров записей
                string[] keys = new string[number];
                int[] indexes = Enumerable.Range(0, (int)number).ToArray();
                // Вычислим и запишем ключи

                for (long ii = 0; ii < number; ii++)
                {
                    keys[ii] = keyfunction(e);
                    e.offset += size;
                }
                // Сортируем два массива
                Array.Sort(keys, indexes);
                ReorderSequenceArrayHeads(number, p0, size, indexes);
            }
            else
            {
            }
        }
        private void MergeUp(long start, long number1, long number2, KeyString keyfunction) 
        {
            if (number1 == 0 || number2 == 0) return;
            PTypeSequence pts = (PTypeSequence)typ;
            int size = pts.ElementType.HeadSize;
            // Заведем файл для верхнего массива 
            string tmp_name = "../../../Databases/temp.bin";
            System.IO.FileStream tmp = new System.IO.FileStream(tmp_name, System.IO.FileMode.Create, System.IO.FileAccess.ReadWrite, System.IO.FileShare.None, 10000);
            // Cкопируем нужное число байтов
            int buff_size = 8192;
            byte[] buffer = new byte[buff_size];
            PxEntry entry1 = this.Element(start);
            long pointer = entry1.offset;
            this.fis.SetOffset(pointer);
            long bytestocopy= number1 * size;
            while (bytestocopy > 0) 
            {
                int nb = this.fis.fs.Read(buffer, 0, bytestocopy < buff_size ? (int)bytestocopy : buff_size); 
                tmp.Write(buffer, 0, nb);
                bytestocopy -= nb;
            }
            // Вычислим и запомним во временной ячейке ключи массива 1.
            // сначала тип ячейки и ячейка:
            PType tp_tmp = new PType(PTypeEnumeration.sstring);
            string tmp_cell_name = "../../../Databases/tmp.pac";
            PaCell tmp_cell = new PaCell(tp_tmp, tmp_cell_name, false);
            // теперь заполним ее значениями
            //tmp_cell.StartSerialFlow();
            //tmp_cell.S();
            //for (long ii=0; ii<number1; ii++) 
            //{
            //    string key = keyfunction(entry1);
            //    tmp_cell.V(key);
            //    entry1.offset += size;
            //}
            //tmp_cell.Se();
            //tmp_cell.EndSerialFlow();
            tmp_cell.Fill(new object[0]);
            for (long ii=0; ii<number1; ii++) 
            {
                string key = keyfunction(entry1);
                tmp_cell.Root.AppendElement(key);
                entry1.offset += size;
            }
            tmp_cell.Flush();
            // теперь будем сливать массивы
            tmp.Position = 0L; // в файле tmp
            long pointer2 = pointer + number1 * size;
            long tmp_offset = tmp_cell.Root.Element(0).offset;
            tmp_cell.SetOffset(tmp_offset);
            PType tp_sstring = new PType(PTypeEnumeration.sstring);
            string key1 = (string)tmp_cell.ScanObject(tp_sstring);
            long cnt1 = 1; // число прочитанных элементов первой подпоследовательности
            PxEntry entry2 = this.Element(start + number1);
            string key2 = keyfunction(entry2);
            long cnt2 = 1; // число прочитанных элементов второй подпоследовательности
            byte[] buff_el = new byte[size * 8192];
            int buff_pos = 0;
            while (cnt1 > number1 || cnt2 > number2) 
            {
                if (key1.CompareTo(key2) < 0) 
                { // Продвигаем первую подпоследовательность
                    tmp.Read(buff_el, buff_pos, size); buff_pos += size;
                    key1 = (string)tmp_cell.ScanObject(tp_sstring);
                    cnt1++;
                } 
                else 
                { // Продвигаем вторую последовательность
                    entry2.fis.SetOffset(entry2.offset);
                    entry2.fis.fs.Read(buff_el, buff_pos, size); buff_pos += size;
                    entry2.offset += size;
                    key2 = keyfunction(entry2);
                    cnt2++;
                }
            }
            // Теперь надо переписать остатки
        }
        public PxEntry BinarySearchFirst(Func<PxEntry, int> elementDepth)
        {
            PxEntry sequ = this;
            var typ = sequ.typ;
            if (typ.Vid != PTypeEnumeration.sequence) throw new Exception("Function FindZero can't be applied to the type with vid=" + typ.Vid);
            PTypeSequence mts = (PTypeSequence)sequ.typ;
            PType tel = mts.ElementType;
            long llen = sequ.Count();
            if (llen == 0) throw new Exception("No elements to FindZero");
            var first_el = sequ.Element(0);
            var first_depth = elementDepth(first_el);
            if (first_depth == 0) return first_el;
            PxEntry found = BSF(first_el, llen, elementDepth);
            //if (found.offset == long.MinValue) throw new Exception("Zero element did't foound by FindZero()");
            return found;
        }
        // В случае неудачи, возвращает PxEntry со значением поля offset == long.MinValue
        // Элемент elementFrom уже проверенный и меньше 0
        private static PxEntry BSF(PxEntry elementFrom, long number, Func<PxEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half == 0) return new PxEntry(null, long.MinValue, null); // Не найден
            var factor = elementFrom.typ.HeadSize;
            PxEntry middle = new PxEntry(elementFrom.typ, elementFrom.offset + half * factor, elementFrom.fis);
            var middle_depth = elementDepth(middle);

            if (middle_depth == 0) return middle;
            if (middle_depth < 0) return BSF(middle, number - half, elementDepth);
            else return BSF(elementFrom, half, elementDepth);
        }
        public IEnumerable<PxEntry> BinarySearchAll(Func<PxEntry, int> elementDepth)
        {
            PxEntry sequ = this;
            var typ = sequ.typ;
            if (typ.Vid != PTypeEnumeration.sequence) throw new Exception("Function FindZero can't be applied to the type with vid=" + typ.Vid);
            PTypeSequence mts = (PTypeSequence)sequ.typ;
            PType tel = mts.ElementType;
            long llen = sequ.Count();
            if (llen > 0)
            {
                var elementFrom = sequ.Element(0);
                foreach (var pe in BinarySearchInside(elementFrom, llen, elementDepth)) yield return pe;
            }
        }
        // Ищет все решения внутри имея ввиду, что слева за диапазоном уровень меньше нуля, справа за диапазоном больше 
        private static IEnumerable<PxEntry> BinarySearchInside(PxEntry elementFrom, long number, Func<PxEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.typ.HeadSize;
                PxEntry middle = new PxEntry(elementFrom.typ, elementFrom.offset + half * size, elementFrom.fis);
                PxEntry aftermiddle = new PxEntry(elementFrom.typ, middle.offset + size, elementFrom.fis);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    foreach (var pe in BinarySearchLeft(elementFrom, half, elementDepth)) yield return pe;
                    yield return middle;
                    foreach (var pe in BinarySearchRight(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else if (middle_depth < 0)
                {
                    foreach (var pe in BinarySearchInside(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else // if (middle_depth > 0)
                {
                    foreach (var pe in BinarySearchInside(elementFrom, half, elementDepth)) yield return pe;
                }
            }
            else if (number == 1) // возможно одно решение или их нет
            {
                if (elementDepth(elementFrom) == 0) yield return elementFrom;
            }
        }
        // Ищет все решения имея ввиду, что справа решения есть 
        private static IEnumerable<PxEntry> BinarySearchLeft(PxEntry elementFrom, long number, Func<PxEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.typ.HeadSize;
                PxEntry middle = new PxEntry(elementFrom.typ, elementFrom.offset + half * size, elementFrom.fis);
                PxEntry aftermiddle = new PxEntry(elementFrom.typ, middle.offset + size, elementFrom.fis);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    foreach (var pe in BinarySearchLeft(elementFrom, half, elementDepth)) yield return pe;
                    yield return middle;
                    // Переписать все из второй половины
                    for (long ii = 0; ii < number - half - 1; ii++)
                    {
                        yield return aftermiddle;
                        aftermiddle = new PxEntry(elementFrom.typ, aftermiddle.offset + size, elementFrom.fis);
                    }
                }
                else if (middle_depth < 0)
                {
                    foreach (var pe in BinarySearchLeft(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else throw new Exception("Assert err: 9283");
            }
            else if (number == 1) // возможно одно решение или их нет
            {
                if (elementDepth(elementFrom) == 0) yield return elementFrom;
            }
        }
        // Ищет все решения имея ввиду, что слева решения есть 
        private static IEnumerable<PxEntry> BinarySearchRight(PxEntry elementFrom, long number, Func<PxEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.typ.HeadSize;
                PxEntry middle = new PxEntry(elementFrom.typ, elementFrom.offset + half * size, elementFrom.fis);
                PxEntry aftermiddle = new PxEntry(elementFrom.typ, middle.offset + size, elementFrom.fis);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    // Переписать все из первой половины
                    PxEntry ef = elementFrom;
                    for (long ii = 0; ii < half; ii++)
                    {
                        yield return ef;
                        ef = new PxEntry(elementFrom.typ, ef.offset + size, elementFrom.fis);
                    }
                    yield return middle;
                    foreach (var pe in BinarySearchRight(aftermiddle, number - half - 1, elementDepth)) yield return pe;
                }
                else if (middle_depth > 0)
                {
                    foreach (var pe in BinarySearchRight(elementFrom, half, elementDepth)) yield return pe;
                }
            }
            else if (number == 1) // возможно одно решение или их нет
            {
                if (elementDepth(elementFrom) == 0) yield return elementFrom;
            }
        }
    }
    public delegate string KeyString(PxEntry x);

}
