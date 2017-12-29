using System;
using System.Collections.Generic;
using Polar.DB;
using Polar.Cells;

namespace Polar.CellIndexes
{
    public interface IBearingTableImmutable
    {
        void Clear();
        void Fill(IEnumerable<object> elements);
        void Scan(Func<long, object, bool> doit); // Сканирует по всем элементам таблицы и выполянет функцию, сканирование прерывается (?) если функция выдаст "ложь"  
        PaEntry Element(long ind); // Нужен в основном для получения ссылки на первый элемент
        //object GetValue(long offset);
        long Count();
      //  void AppendValue(object[] objects);
    }
    public interface IIndexImmutable<Tkey>
    {
        Func<object, Tkey> KeyProducer { get; set; }
        //Func<Tkey, int> Level = key => key.CompareTo(key0);
        //IBearingTableImmutable Table { get; set; }
        void Build();
        // Диапазон - диапазон в индексном массиве
        IEnumerable<PaEntry> GetAllByKey(long start, long number, Tkey key);
        IEnumerable<PaEntry> GetAllByKey(Tkey key);
        long Count();
        void Warmup();
        void ActivateCache();
        // Доступ к индексному массиву
        PaCell IndexCell { get; }
        IScale Scale { get; set; }
        IBearingTable Table { get; set; }
        void FillPortion(IEnumerable<TableRow> tableRows);
        void FillFinish();
        void FillInit();
        IEnumerable<PaEntry> GetAllByLevel(Func<PaEntry, int> levelFunc);
    }
    public interface IBearingTable : IBearingTableImmutable
    {
        // Системные действия по ведению индексов
        void RegisterIndex(IIndexCommon index);
        void UnregisterIndex(IIndexCommon index);
        PaEntry AppendValue(object value);
        void DeleteEntry(PaEntry record);
        IEnumerable<PaEntry> GetUndeleted(IEnumerable<PaEntry> elements);
        void Warmup();
        //void ActivateCache();
    }
    public interface IIndexCommon
    {
        void OnAppendElement(PaEntry entry);
        void DropIndex();
        void Build();
        void Clear();
        void FillPortion(IEnumerable<TableRow> tableRows);
        void FillFinish();
        void FillInit();
        void Warmup();
    }
    public interface IIndex<Tkey> : IIndexImmutable<Tkey>, IIndexCommon
    {
        // Найти все, соответствующие нулевому уровню
        IEnumerable<PaEntry> GetAllByLevel(Func<Tkey,int> LevelFunc);
    }
    public interface IScale
    {
        PaCell IndexCell { get; set; }
        void Build();
        void Build(long n);
        Diapason GetDiapason(int key);
        void Warmup();
        void ActivateCache();
    }
}
