using System;
using System.Collections.Generic;
using System.Text;

namespace Polar.DB
{
    /// <summary>
    /// Интерфейс специальной "поддерживающей" последоваельности. Последовательность состоит из элементов,
    /// которые могут быть "обернуты" во что-то, напр. может быть добавлен признак deleted. Элементы вместе
    /// с оберткой называются "полными". Но могут быть и только элементы
    /// </summary>
    public interface IBearing
    {
        /// <summary>
        /// Очистка последовательности
        /// </summary>
        void Clear();
        
        /// <summary>
        /// Сброс буферов, важная операция, когда идет добавление по-элементно 
        /// </summary>
        void Flush();

        /// <summary>
        /// Загрузка данных и вычисление индексов
        /// </summary>
        /// <param name="flow"></param>
        void Load(IEnumerable<object> flow);

        /// <summary>
        /// Построение или перепостроение индексов
        /// </summary>
        void Build();

        /// <summary>
        /// "освежение" или "разогрев" - важна после подсоединения к уже сформированной последовательности 
        /// </summary>
        void Refresh();
        /// <summary>
        /// Выдает поток "чистых" значений, уничтоженные не попадают в поток
        /// </summary>
        /// <returns></returns>
        IEnumerable<object> ElementValues();
        
        /// <summary>
        /// Сканирует все (полные) элементы, пропускает "уничтоженные", выдает хендлеру офсет полного элемента, 
        /// чистый объект, сканирование продолжается пока результат хендлера true 
        /// </summary>
        /// <param name="handler"></param>
        void Scan(Func<long, object, bool> handler);
        
        /// <summary>
        /// Соответствует GetElement, но извлекает чистый элемент если он в обертке 
        /// </summary>
        /// <param name="off"></param>
        /// <returns></returns>
        object GetItem(long off);

        /// <summary>
        /// упаковывает и добавляет к опорной последовательности новый элемент (айтем) 
        /// </summary>
        /// <param name="item"></param>
        /// <returns>Возвращает офсет записанного элемента</returns>
        long AddItem(object item);

        /// <summary>
        /// Уничтожает айтем по офсету элемента, в который айтем упакован 
        /// </summary>
        /// <param name="off"></param>
        void DeleteItem(long off);

        /// <summary>
        /// Массив индексов, сопоставленных опорной последовательности
        /// </summary>
        IIndex[] Indexes { get; set; }
    }
}
