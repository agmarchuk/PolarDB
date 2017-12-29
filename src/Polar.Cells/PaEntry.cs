using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Cells
{
    public class PaEntry
    {
        public long offset;
        private PType tp;
        public PType Type { get { return tp; } }
        private PaCell cell;

        // Пустое значение. Часто используется в качестве указания, что вход не найден: 
        // var found = entry.Where(en => predicate).DefaultIfEmpty(PaEntry.Empty).First(); if (found.IsEmpty()) ...
        private static PaEntry _empty = new PaEntry(null, Int64.MinValue, null);
        public static PaEntry Empty { get { return _empty; } }
        public static bool IsNullOrEmpty(PaEntry ent) { return ent == null || ent.offset == Int64.MinValue; }
        public bool IsEmpty { get { return offset == Int64.MinValue; } }

        public PaEntry(PType tp, long offset, PaCell cell)
        {
            this.tp = tp;
            this.offset = offset;
            this.cell = cell;
        }
        public PaEntry SetOffset(long offset) { this.offset = offset; return this; }

        // ===============================================
        // ============== Методы доступа =================
        // ===============================================

        // ========== Для записей =========
        public PaEntry Field(int index)
        {
            if (tp.Vid != PTypeEnumeration.record) throw new Exception("Err in TPath formula: Field can't be applied to structure of vid " + tp.Vid);
            PTypeRecord mtr = (PTypeRecord)tp;
            if (index >= mtr.Fields.Length) throw new Exception("Err in TPath formula: index of Field is too large " + index);

            long pos = this.offset;
            for (int i = 0; i < index; i++)
            {
                PType t = mtr.Fields[i].Type;
                if (t.HasNoTail) pos += t.HeadSize;
                else
                {
                    pos = Skip(t, pos);
                }
            }
            return new PaEntry(mtr.Fields[index].Type, pos, cell);
        }
        // ========== Для последовательностей =========
        // Подсчитывает число элементов последовательности
        public long Count()
        {
            if (tp.Vid != PTypeEnumeration.sequence) throw new Exception("Err in TPath formula: Count() can't be applyed to structure of vid " + tp.Vid);
            return cell.ReadCount(this.offset);
        }
        public PaEntry Element(long index)
        {
            if (tp.Vid != PTypeEnumeration.sequence) throw new Exception("Err in TPath formula: Element() can't be applyed to structure of vid " + tp.Vid);
            PTypeSequence mts = (PTypeSequence)tp;
            PType t = mts.ElementType;
            long llen = this.Count();
            if (index < 0 || index >= llen) throw new Exception("Err in TPath formula: wrong index of Element " + index);
            // для внешних равноэлементных последовательностей - специальная формула
            if (t.HasNoTail || index == 0) return new PaEntry(t, this.offset + 8 + index * t.HeadSize, cell);
            //cell.SetOffset(this.offset); //Убрал
            long pos = this.offset + 8;
            for (long ii = 0; ii < index; ii++)
            {
                pos = Skip(t, pos);
            }
            return new PaEntry(t, pos, cell);
        }
        // TODO: Рассмотреть целесообразность этого метода
        private PaEntry ElementUnchecked(long index)
        {
            PType t = ((PTypeSequence)tp).ElementType;
            if (t.HasNoTail) return new PaEntry(t, this.offset + 8 + index * t.HeadSize, cell);
            long pos = this.offset + 8;
            for (long ii = 0; ii < index; ii++)
            {
                pos = Skip(t, pos);
            }
            return new PaEntry(t, pos, cell);
        }
        public IEnumerable<PaEntry> Elements()
        {
            return Elements(0, this.Count());
        }
        public IEnumerable<PaEntry> Elements(long start, long number)
        {
            if (tp.Vid != PTypeEnumeration.sequence) throw new Exception("Err in TPath formula: Elements() can't be applyed to structure of vid " + tp.Vid);
            if (number > 0)
            {
                PTypeSequence mts = (PTypeSequence)tp;
                PType t = mts.ElementType;
                PaEntry element = this.Element(start);
                if (t.HasNoTail)
                {
                    int size = t.HeadSize;
                    for (long ii = 0; ii < number; ii++)
                    {
                        yield return element;
                        element.offset += size;
                    }
                }
                else
                {
                    long off = element.offset;
                    for (long ii = 0; ii < number; ii++)
                    {
                        element.offset = off;
                        if (ii < number - 1) off = element.Skip(t, off);
                        yield return element;
                    }
                }
            }
        }
        // ========== Для объединений =========
        public int Tag()
        {
            if (tp.Vid != PTypeEnumeration.union) throw new Exception("Err: Tag() needs union");
            return cell.ReadByte(this.offset);
        }
        public PaEntry UElement()
        {
            int tag = this.Tag();
            PTypeUnion ptu = ((PTypeUnion)this.tp);
            if (tag < 0 || tag >= ptu.Variants.Length) throw new Exception("Err: tag is out of bound");
            PType tel = ptu.Variants[tag].Type;
            return new PaEntry(tel, offset + 1, cell);
        }
        public PaEntry UElementUnchecked(int tag)
        {
            PTypeUnion ptu = ((PTypeUnion)this.tp);
            if (tag < 0 || tag >= ptu.Variants.Length) throw new Exception("Err: tag is out of bound");
            PType tel = ptu.Variants[tag].Type;
            return new PaEntry(tel, offset + 1, cell);
        }

        public PValue GetValue()
        {
            return new PValue(this.tp, this.offset, Get());
        }
        public object Get()
        {
            return cell.GetPObject(tp, this.offset);
        }
        public void Set(object valu)
        {
            this.cell.SetPObject(this.Type, this.offset, valu);
        }
        public long AppendElement(object po)
        {
            if (this.cell.IsEmpty || this.offset != this.cell.Root.offset || this.Type.Vid != PTypeEnumeration.sequence)
            { throw new Exception("AppendElement for PaCell must be applied to root sequence and nonempty cell"); }
            return cell.AppendPObj(((PTypeSequence)this.Type).ElementType, po);
        }

        // Следующие два метода возможно имеют побочный эффект
        public IEnumerable<object> ElementValues()
        {
            return ElementValues(0, this.Count());
        }
        public IEnumerable<object> ElementValues(long start, long number)
        {
            if (tp.Vid != PTypeEnumeration.sequence) throw new Exception("Err in TPath formula: ElementValues() can't be applyed to structure of vid " + tp.Vid);
            PType t = ((PTypeSequence)tp).ElementType;
            long ll = this.Count();
            // Надо проверить соответствие диапазона количеству элементов
            if (start < 0 || start + number > ll) throw new Exception("Err: Diapason is out of range");
            if (number == 0) yield break;
            PaEntry first = this.Element(start);
            long off = first.offset;
            for (long ii = 0; ii < number; ii++)
            {
                long offout;
                object v = cell.GetPObject(t, off, out offout);
                off = offout;
                yield return v;
            }
        }
        // Основной сканер: быстро пробегаем по элементам, обрабатываем пары (offset, pobject), возвращаем true
        public void Scan(Func<long, object, bool> handler)
        {
            if (tp.Vid != PTypeEnumeration.sequence) throw new Exception("Err in TPath formula: ElementValues() can't be applyed to structure of vid " + tp.Vid);
            PTypeSequence mts = (PTypeSequence)tp;
            PType t = mts.ElementType;
            long ll = this.Count();
            if (ll == 0) return;
            PaEntry first = this.Element(0);
            long off = first.offset;
            for (long ii = 0; ii < ll; ii++)
            {
                long offout;
                object pobject = cell.GetPObject(t, off, out offout);
                bool ok = handler(off, pobject);
                off = offout;
                if (!ok) throw new Exception("Scan handler catched 'false' at element " + ii);
            }
        }

        // Техническая процедура Пропускает поле, выдает адрес, следующий за ним. Указатель никуда не установлен 
        private long Skip(PType tp, long off)
        {
            if (tp.HasNoTail) return off + tp.HeadSize;
            if (tp.Vid == PTypeEnumeration.sstring)
            {
                long offout;
                cell.ReadString(off, out offout);
                return offout;
            }
            if (tp.Vid == PTypeEnumeration.record)
            {
                long field_offset = off;
                PTypeRecord mtr = (PTypeRecord)tp;
                foreach (var pa in mtr.Fields)
                {
                    field_offset = Skip(pa.Type, field_offset);
                }
                return field_offset;
            }
            if (tp.Vid == PTypeEnumeration.sequence)
            {
                PTypeSequence mts = (PTypeSequence)tp;
                PType tel = mts.ElementType;
                long llen = cell.ReadLong(off);
                if (tel.HasNoTail) return off + 8 + llen * tel.HeadSize;
                long element_offset = off + 8;
                for (long ii = 0; ii < llen; ii++) element_offset = Skip(tel, element_offset);
                return element_offset;
            }
            if (tp.Vid == PTypeEnumeration.union)
            {
                PTypeUnion mtu = (PTypeUnion)tp;
                int v = cell.ReadByte(off);
                if (v < 0 || v >= mtu.Variants.Length) throw new Exception("Err in Skip (TPath-formula): wrong variant for union " + v);
                PType mt = mtu.Variants[v].Type;
                return Skip(mt, off + 1);
            }
            throw new Exception("Assert err: 2874");
        }

        //=============== Сортировка по ключу ===============
        public static long bufferBytes = 200 * 1000 * 1000;
        public void SortByKey<Tkey>(Func<object, Tkey> keyfunction, IComparer<Tkey> comparer = null)
        {
            if (tp.Vid != PTypeEnumeration.sequence) throw new Exception("SortByKey can't be implemented to this vid");
            PTypeSequence pts = (PTypeSequence)tp;
            if (!pts.ElementType.HasNoTail) throw new Exception("SortByKey can't be implemented to this type");
            long llen = this.Count();
            SortByKey(0, llen, keyfunction, comparer);
        }
        public void SortByKey<Tkey>(long start, long number, Func<object, Tkey> keyfunction,
             IComparer<Tkey> comparer)
        {
            PTypeSequence pts = (PTypeSequence)this.Type;
            if (number < 2) return; // сортировать не нужно
            int size = pts.ElementType.HeadSize;
            long bufferSize = bufferBytes / size; // Это неправильно, правильнее - "зацепиться" за размер Tkey
            if (number <= bufferSize)
            {
                // Указатель на начальный элемент и размер головы записи
                PaEntry e = this.Element(start);
                // организуем массивы значений элементов и ключей
                object[] elements = new object[number];
                Tkey[] keys = new Tkey[number];
                // Вычислим и запишем значения и ключи
                for (long ii = 0; ii < number; ii++)
                {
                    var v = e.Get();
                    elements[ii] = v;
                    keys[ii] = keyfunction(v);
                    e.offset += size;
                }
                // Сортируем два массива
                Array.Sort(keys, elements, comparer);
                // Возвращаем значения
                e = this.Element(start);
                for (long ii = 0; ii < number; ii++)
                {
                    e.Set(elements[ii]);
                    e.offset += size;
                }
            }
            else
            {
                long half = number / 2;
                SortByKey<Tkey>(start, half, keyfunction, comparer);
                SortByKey<Tkey>(start + half, number - half, keyfunction, comparer);

                MergeUpByKey<Tkey>(start, half, number - half, keyfunction, comparer);
                Console.WriteLine("MergeUpByKey {0} values", number);
            }
        }
        public void MergeUpByKey<Tkey>(long start, long number1, long number2, Func<object, Tkey> keyfunction,
             IComparer<Tkey> comparer)
        {
            if (number1 == 0 || number2 == 0) return;
            PTypeSequence pts = (PTypeSequence)tp;
            PType tel = pts.ElementType;


            PaEntry entry = this.Element(start); // Вход в начальный элемент
            long off1 = entry.offset;
            PaEntry entry2 = this.Element(start + number1); // Вход в начальный элемент второй группы
            long off2 = entry2.offset;

            IComparer<Tkey> compar = comparer;
            if (compar == null) compar = Comparer<Tkey>.Default;
            Func<object, object, int> comparePObj = (object o1, object o2) =>
            {
                return compar.Compare(keyfunction(o1), keyfunction(o2));
            };
            this.cell.CombineParts(tel, off1, number1, off2, number2, comparePObj);
        }

        /// <summary>
        /// "Поиск" индекса элемента в последовательности. Работает только для выровненых последовательностей, 
        /// индекс вычисляется арифметическими действиями с использованием размера элемента
        /// </summary>
        /// <param name="element">Указатель на элемент</param>
        /// <returns>Индекс элемента или исключение IndexOutOfRangeException</returns>
        public long IndexOf(PaEntry element)
        {
            if (this.Type.Vid != PTypeEnumeration.sequence) throw new Exception("Method IndexOf can't be implemented to this type");
            PType tel = ((PTypeSequence)this.Type).ElementType;
            if (!tel.HasNoTail) throw new Exception("Method IndexOf can't be implemented ot this element type");
            long ind = (element.offset - this.offset - 8) / tel.HeadSize;
            if (ind < 0 || ind >= this.Count()) throw new IndexOutOfRangeException();
            return ind;
        }


        #region binary search by elementDepth
        public PaEntry BinarySearchFirst(Func<PaEntry, int> elementDepth)
        {
            PaEntry sequ = this;
            var typ = sequ.Type;
            if (typ.Vid != PTypeEnumeration.sequence) throw new Exception("Function BinarySearchFirst can't be applied to the type with vid=" + typ.Vid);
            PTypeSequence mts = (PTypeSequence)sequ.Type;
            PType tel = mts.ElementType;
            if (!tel.HasNoTail) throw new Exception("Function BinarySearchFirst can't be applied to elements with vid=" + tel.Vid);
            long llen = sequ.Count();
            if (llen == 0) { sequ.offset = Int64.MinValue; return sequ; }
            var first_el = sequ.Element(0);
            var first_depth = elementDepth(first_el);
            if (first_depth == 0) return first_el;
            PaEntry found = BinarySearchFirst(first_el, llen, elementDepth);
            //if (found.offset == long.MinValue) throw new Exception("Zero element did't foound by FindZero()");
            return found;
        }
        public PaEntry BinarySearchFirst(long start, long number, Func<PaEntry, int> elementDepth)
        {
            PaEntry sequ = this;
            if (this.Type.Vid != PTypeEnumeration.sequence) throw new Exception("Function BinarySearchFirst can't be applied to this type");
            PTypeSequence mts = (PTypeSequence)sequ.Type;
            PType tel = mts.ElementType;
            if (!tel.HasNoTail) throw new Exception("Function BinarySearchFirst can't be applied to elements with vid=" + tel.Vid);
            long llen = sequ.Count();
            if (llen == 0 || number == 0) { return PaEntry.Empty; }
            if (start < 0 || number < 0 || start + number > llen) throw new IndexOutOfRangeException();
            var first_el = sequ.Element(start);
            var first_depth = elementDepth(first_el);
            if (first_depth == 0) return first_el;
            PaEntry found = BinarySearchFirst(first_el, number, elementDepth);
            return found;
        }
        // В случае неудачи, возвращает PxEntry со значением поля offset == long.MinValue
        // Элемент elementFrom уже проверенный и меньше 0
        private static PaEntry BinarySearchFirst(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half == 0) return new PaEntry(null, long.MinValue, null); // Не найден
            var factor = elementFrom.Type.HeadSize;
            PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * factor, elementFrom.cell);
            var middle_depth = elementDepth(middle);

            if (middle_depth == 0) return middle;
            if (middle_depth < 0)
            {
                return BinarySearchFirst(middle, number - half, elementDepth);
            }
            else
            {
                return BinarySearchFirst(elementFrom, half, elementDepth);
            }
        }
        public IEnumerable<PaEntry> BinarySearchAll(Func<PaEntry, int> elementDepth)
        {
            return BinarySearchAll(0, this.Count(), elementDepth);
        }
        public IEnumerable<PaEntry> BinarySearchAll(long start, long numb, Func<PaEntry, int> elementDepth)
        {
            PaEntry sequ = this;
            var typ = sequ.Type;
            if (typ.Vid != PTypeEnumeration.sequence) throw new Exception("Function BinarySearchAll can't be applied to the type with vid=" + typ.Vid);
            PTypeSequence mts = (PTypeSequence)sequ.Type;
            PType tel = mts.ElementType;
            long llen = System.Math.Min(numb, sequ.Count() - start);
            if (llen > 0)
            {
                var elementFrom = sequ.Element(start);
                //foreach (var pe in BinarySearchInside(elementFrom, llen, elementDepth)) yield return pe;
                return BinarySearchInside(elementFrom, llen, elementDepth);
            }
            return Enumerable.Empty<PaEntry>();
        }
        // Ищет все решения внутри имея ввиду, что слева за диапазоном уровень меньше нуля, справа за диапазоном больше 
        private static IEnumerable<PaEntry> BinarySearchInside(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.Type.HeadSize;
                PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
                PaEntry aftermiddle = new PaEntry(elementFrom.Type, middle.offset + size, elementFrom.cell);
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
            else if (number == 1) // && half == 0) - возможно одно решение или их нет
            {
                if (elementDepth(elementFrom) == 0) yield return elementFrom;
            }
        }
        // Ищет все решения имея ввиду, что справа решения есть 
        private static IEnumerable<PaEntry> BinarySearchLeft(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.Type.HeadSize;
                PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
                PaEntry aftermiddle = new PaEntry(elementFrom.Type, middle.offset + size, elementFrom.cell);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    foreach (var pe in BinarySearchLeft(elementFrom, half, elementDepth)) yield return pe;
                    yield return middle;
                    // Переписать все из второй половины
                    for (long ii = 0; ii < number - half - 1; ii++)
                    {
                        yield return aftermiddle;
                        aftermiddle = new PaEntry(elementFrom.Type, aftermiddle.offset + size, elementFrom.cell);
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
        private static IEnumerable<PaEntry> BinarySearchRight(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            long half = number / 2;
            if (half > 0)
            {
                var size = elementFrom.Type.HeadSize;
                PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
                PaEntry aftermiddle = new PaEntry(elementFrom.Type, middle.offset + size, elementFrom.cell);
                var middle_depth = elementDepth(middle);

                if (middle_depth == 0)
                {
                    // Переписать все из первой половины
                    PaEntry ef = elementFrom;
                    for (long ii = 0; ii < half; ii++)
                    {
                        yield return ef;
                        ef = new PaEntry(elementFrom.Type, ef.offset + size, elementFrom.cell);
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

        // ==================== Поиск диапазона ===================

        public Diapason BinarySearchDiapason(Func<PaEntry, int> elementDepth)
        {
            long nelements = this.Count();
            if (nelements == 0) return Diapason.Empty;
            PaEntry first = this.Element(0);
            if (elementDepth(first) > 0) return Diapason.Empty;
            // Теперь первый элемент проверенный и первый <= 0
            var dia = BinarySearchDiapason(first, nelements, elementDepth);
            if (dia.numb == 0) return new Diapason() { start = 0, numb = 0 };
            long ind = this.IndexOf(dia.first);
            return new Diapason() { start = ind, numb = dia.numb };
        }
        public Diapason BinarySearchDiapason(long start, long number, Func<PaEntry, int> elementDepth)
        {
            long nelements = this.Count();
            long numb = start + number > nelements ? nelements - start : number;
            if (numb <= 0 || start < 0 || start >= nelements) return Diapason.Empty;
            PaEntry first = this.Element(start);
            if (elementDepth(first) > 0) return Diapason.Empty;
            // Теперь первый элемент проверенный и первый <= 0
            var dia = BinarySearchDiapason(first, numb, elementDepth);
            if (dia.IsEmpty()) return Diapason.Empty;
            long ind = this.IndexOf(dia.first);
            return new Diapason() { start = ind, numb = dia.numb };
        }
        // Вычисляет поддиапазон значений, удовлетворяющих условию elementDepth(e) == 0 
        // Элемент elementFrom уже проверенный и <= 0 -- откажусь от этого условия, оно ничего не дает
        private static DiapasonFromEntry BinarySearchDiapason(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            if (number <= 0) return DiapasonFromEntry.Empty;
            if (number == 1)
            {
                if (elementDepth(elementFrom) == 0) return new DiapasonFromEntry() { first = elementFrom, numb = 1 };
                else return DiapasonFromEntry.Empty;
            }
            long half = number / 2;
            var size = elementFrom.Type.HeadSize;
            PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
            var middle_depth = elementDepth(middle);

            if (middle_depth == 0)
            {
                long countLeft = BinaryCountLeft(elementFrom, half, elementDepth);
                middle.offset += size; // Начальный элемент будет следующий 
                long countRight = BinaryCountRight(middle, number - half - 1, elementDepth);
                PaEntry startFrom = new PaEntry(
                    elementFrom.Type,
                    middle.offset - countLeft * size - size,
                    elementFrom.cell);
                return new DiapasonFromEntry() { first = startFrom, numb = countLeft + 1 + countRight };
            }
            else if (middle_depth < 0)
            {
                middle.offset += size; // Начальный элемент будет следующий 
                return BinarySearchDiapason(middle, number - half - 1, elementDepth);
            }
            else // if (middle_depth > 0)
            {
                return BinarySearchDiapason(elementFrom, half, elementDepth);
            }
        }
        // подсчитывает число "нулевых" элементов справа налево
        private static long BinaryCountLeft(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            if (number <= 0) return 0;
            if (number == 1)
            {
                if (elementDepth(elementFrom) == 0) return 1;
                else return 0;
            }
            long half = number / 2;
            var size = elementFrom.Type.HeadSize;
            PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
            var middle_depth = elementDepth(middle);

            if (middle_depth == 0)
            {
                return BinaryCountLeft(elementFrom, half, elementDepth) + number - half;
            }
            else if (middle_depth < 0)
            {
                middle.offset += size; // Начальный элемент будет следующий 
                return BinaryCountLeft(middle, number - half - 1, elementDepth);
            }
            else // if (middle_depth > 0)
            {
                throw new Exception("Assert Err: 2984");
            }
        }
        // подсчитывает число "нулевых" элементов слева направо
        private static long BinaryCountRight(PaEntry elementFrom, long number, Func<PaEntry, int> elementDepth)
        {
            if (number <= 0) return 0;
            if (number == 1)
            {
                if (elementDepth(elementFrom) == 0) return 1;
                else return 0;
            }
            long half = number / 2;
            var size = elementFrom.Type.HeadSize;
            PaEntry middle = new PaEntry(elementFrom.Type, elementFrom.offset + half * size, elementFrom.cell);
            var middle_depth = elementDepth(middle);

            if (middle_depth == 0)
            {
                middle.offset += size; // Начальный элемент будет следующий 
                return half + 1 + BinaryCountRight(middle, number - half - 1, elementDepth);
            }
            else if (middle_depth > 0)
            {
                return BinaryCountRight(elementFrom, half, elementDepth);
            }
            else // if (middle_depth < 0)
            {
                throw new Exception("Assert Err: 2984");
            }
        }
        #endregion


    }
    //public struct Diapason
    //{
    //    public long start, numb; // Инициализируются нулевые значения полей
    //    public static Diapason Empty { get { return new Diapason() { numb = 0, start = Int64.MinValue }; } }
    //    public bool IsEmpty() { return numb <= 0; }
    //}
    internal class DiapasonFromEntry
    {
        public PaEntry first;
        public long numb;
        public static DiapasonFromEntry Empty { get { return new DiapasonFromEntry() { numb = 0 }; } }
        public bool IsEmpty() { return numb <= 0; }
    }
}
