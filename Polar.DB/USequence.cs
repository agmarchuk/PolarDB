using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Universal
{
    public class USequence
    {
        // У универсальной последовательности нет динамической части. Все элементы доступны через методы.
        // Однако элемент может быть пустым. 
        private UniversalSequenceBase sequence;
        private Func<object, bool> isEmpty;
        private Func<object, IComparable> keyFunc;
        private UKeyIndex primaryKeyIndex;
        public IUIndex[] uindexes { get; set; } = new IUIndex[0];
        private bool optimise = true;
        
        public USequence(PType tp_el, string? stateFileName, Func<Stream> streamGen, Func<object, bool> isEmpty,
            Func<object, IComparable> keyFunc, Func<IComparable, int> hashOfKey, bool optimise = true)
        {
            sequence = new UniversalSequenceBase(tp_el, streamGen());
            this.isEmpty = isEmpty;
            this.keyFunc = keyFunc;
            this.optimise = optimise;
            this.stateFileName = stateFileName;
            primaryKeyIndex = new UKeyIndex(streamGen, this, keyFunc, hashOfKey, optimise);
        }

        // Файл для сохранения параметров состояния. Команда сохранения выполняется в конце Load()
        // Имя файла может быть null, тогда это означает, что состояние не фиксируется и не восстанавливается
        private string? stateFileName;
        
        // Следующий метод актуален только если statefile != null
        public void RestoreDynamic()
        {
            FileStream statefile = new FileStream(stateFileName, FileMode.OpenOrCreate, FileAccess.Read);
            BinaryReader reader = new BinaryReader(statefile);
            long statenelements = reader.ReadInt64(); //old sequence.Count();
            long elementoffset = reader.ReadInt64(); // sequence.ElementOffset();
            statefile.Close();
            // А текущий размер:
            long nelements = sequence.Count();
            // Динамику надо воспроизводить только если размер увеличился
            Console.WriteLine($"{nelements - statenelements} elements added");
            if (nelements > statenelements)
            {
                var additional = sequence.ElementOffsetValuePairs(elementoffset, nelements - statenelements);
                foreach (var pair in additional)
                {
                    primaryKeyIndex.OnAppendElement(pair.Item2, pair.Item1);
                    if (uindexes != null) foreach (var uind in uindexes) uind.OnAppendElement(pair.Item2, pair.Item1);
                }
            }
        }

        public void Clear() { sequence.Clear(); primaryKeyIndex.Clear(); if (uindexes != null) foreach (var ui in uindexes) ui.Clear(); }
        public void Flush() { sequence.Flush(); primaryKeyIndex.Flush(); if (uindexes != null) foreach (var ui in uindexes) ui.Flush(); }
        public void Close() { sequence.Close(); primaryKeyIndex.Close(); if (uindexes != null) foreach (var ui in uindexes) ui.Close(); }
        public void Refresh() { sequence.Refresh(); primaryKeyIndex.Refresh(); if (uindexes != null) foreach (var ui in uindexes) ui.Refresh(); }

        public void Load(IEnumerable<object> flow)
        {
            Clear();
            foreach (var element in flow)
            {
                if (!isEmpty(element)) sequence.AppendElement(element);
            }
            Flush();

            if (stateFileName != null)
            {
                // =========== Зафиксируем состояние в файле. Запомним текущее число элементов и офсет следующего ====
                FileStream statefile = new FileStream(stateFileName, FileMode.OpenOrCreate, FileAccess.Write);
                BinaryWriter writer = new BinaryWriter(statefile);
                writer.Write(sequence.Count());
                writer.Write(sequence.ElementOffset());
                statefile.Close();
            }
        }
        internal bool IsOriginalAndNotEmpty(object element, long off) =>
            primaryKeyIndex.IsOriginal(keyFunc(element), off) && !isEmpty(element); // сначала на оригинал, потом на пустое, может можно и иначе 


        public IEnumerable<object> ElementValues()
        {
            return sequence.ElementOffsetValuePairs()
                // Оставляем оригиналы и непустые
                .Where(pair => IsOriginalAndNotEmpty(pair.Item2, pair.Item1))
                .Select(pair => pair.Item2);
        }
        public void Scan(Func<long, object, bool> handler)
        {
            sequence.Scan((off, ob) => 
            {
                if (IsOriginalAndNotEmpty(ob, off)) 
                {
                    bool ok = handler(off, ob);
                    return ok;
                } 
                return true; // Реакция на не оригинал или пустой
            });
        }
        public void AppendElement(object element)
        {
            long off = sequence.AppendElement(element);
            // Корректировка индексов
            primaryKeyIndex.OnAppendElement(element, off);
            if (uindexes != null) foreach (var uind in uindexes) uind.OnAppendElement(element, off);
        }
        public void CorrectOnAppendElement(long off)
        {
            object element = sequence.GetElement(off);
            // Корректировка индексов
            primaryKeyIndex.OnAppendElement(element, off);
            if (uindexes != null) foreach (var uind in uindexes) uind.OnAppendElement(element, off);
        }

        public object GetByKey(IComparable keysample)
        {
            return primaryKeyIndex.GetByKey(keysample);
        }

        internal object GetByOffset(long off)
        {
            return sequence.GetElement(off);
        }

        public IEnumerable<object> GetAllByValue(int nom, IComparable value,
            Func<object, IEnumerable<IComparable>> keysFunc, bool ignorecase = false)
        {
            if (uindexes[nom] is SVectorIndex)
            {
                var sind = (SVectorIndex)uindexes[nom];
                IEnumerable<object> query = sind.GetAllByValue((string)value)
                    .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                    .Select(obof => obof.obj)
                    //.Select(ob => ConvertNaming(ob))
                    ;
                return query;
            }
            if (uindexes[nom] is UVectorIndex)
            {
                var uind = (UVectorIndex)uindexes[nom];
                IEnumerable<object> query = uind.GetAllByValue((IComparable)value)
                    .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                    .Select(obof => obof.obj)
                    ;
                return query;
            }
            if (uindexes[nom] is UVecIndex)
            {
                var uvind = (UVecIndex)uindexes[nom];

                IEnumerable<object> query = uvind.GetAllByValue(value)
                    .Where(obof => keysFunc(obof.obj)
                        .Select(w => ignorecase ? ((string)w).ToUpper() : w)
                        .Any(W => W.CompareTo(value) == 0))
                    .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                    .Select(obof => obof.obj)
                    .ToArray();
                return query;
            }
            else throw new Exception("93394");
        }
        public IEnumerable<object> GetAllBySample(int nom, object osample)
        {
            if (uindexes[nom] is UIndex)
            {
                var uind = (UIndex)uindexes[nom];
                IEnumerable<object> query = uind.GetAllBySample(osample)
                    .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                    .Select(obof => obof.obj)
                    //.Select(ob => ConvertNaming(ob))
                    ;
                return query;
            }
            else throw new Exception("93394");
        }
        public IEnumerable<object> GetAllByLike(int nom, object sample)
        {
            var uind = uindexes[nom];
            if (uind is SVectorIndex)
            {
                var query = ((SVectorIndex)uind).GetAllByLike((string)sample)
                    .Where(obof => IsOriginalAndNotEmpty(obof.obj, obof.off))
                    .Select(obof => obof.obj) // 
                    //.Select(ob => ConvertNaming(ob))
                    ;
                return query;
            }
            throw new NotImplementedException("Err: 292121");
        }

        public void Build()
        {
            this.primaryKeyIndex.Build();
            foreach (var ind in uindexes) ind.Build();
        }
    }
}
