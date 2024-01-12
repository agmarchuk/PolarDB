using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Polar.PagedStreams
{
    /// <summary>
    /// Класс определяет абстракцию множества блоков (страниц) одинакового размера, доступных для формирования страничных систем.
    /// Координатной "осью" является длинное (long) целое, к котором кодируется некоторый целочисленный номер блока (страницы)
    /// и смещение в блоке. байты внутри блока расположены подряд от байта с нулевой относительной позицией до байта с
    /// позицией BlockSize - 1. 
    /// </summary>
    public abstract class SetOfBlocks
    {
        /// <summary>
        /// Выдает длину блока. 
        /// </summary>
        public abstract long BlockSize { get; }
        /// <summary>
        /// Выдает индекс блока. Индекс по умолчанию - смещение, деленное на длину блока
        /// </summary>
        /// <param name="off"></param>
        /// <returns></returns>
        public virtual long BlockIndex(long off) { return off / BlockSize; }
        /// <summary>
        /// Выдает позицию офсета относительно начала блока. По умолчанию, это остаток от деления офсета на длину блока
        /// </summary>
        /// <param name="off"></param>
        /// <returns></returns>
        public virtual long LocalPosition(long off) { return off % BlockSize; }
        
        /// <summary>
        /// Выдает офсет (в множестве) первого блока. Для простого файла это будет 0L
        /// </summary>
        /// <returns></returns>
        public abstract long GetFirst();

        /// <summary>
        /// Предполагается, что он выдает первый блок как отдельный стрим. 
        /// Думаю, что длина стрима не должна быть существенной. Если SOB построен на базе обычного файла, допустимо
        /// в качестве результата выдавать сам Stream файла. Ответственность программиста - не использвать в таком 
        /// стриме больше байтов, чем длина блока. Стрим начинается с 9 байт - количества блоков, потом идет полезная информация  
        /// </summary>
        /// <returns></returns>
        public abstract System.IO.Stream GetFirstAsStream();

        /// <summary>
        /// Выдает офсет следующего пустого блока, который может быть задействован в конструкции хранилища и добавляет его в хранилище
        /// </summary>
        /// <returns></returns>
        public abstract long GetNext();

        /// <summary>
        /// Сбрасывает внутренние буфера записи, если они есть
        /// </summary>
        public abstract void Flush();

        /// <summary>
        /// Переписывает count байтов из блока с координатой coord в буфер, начиная с позиции buf_offset.
        /// Диапазон count должен быть внутри блока, в который "попали", т.е. цепочка байтов - непрерывна
        /// </summary>
        /// <param name="coord"></param>
        /// <param name="buffer"></param>
        /// <param name="buf_offset"></param>
        /// <param name="count"></param>
        /// <returns>возвращает либо count, либо сколько переписано.</returns>
        internal abstract int Rd(long coord, byte[] buffer, int buf_offset, int count);

        /// <summary>
        /// Переписывает count байтов в блочную память с координатой coord из буфера, начиная с позиции buf_offset
        /// </summary>
        /// <param name="coord"></param>
        /// <param name="buffer"></param>
        /// <param name="buf_offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        internal abstract void Wr(long coord, byte[] buffer, int buf_offset, int count);

        internal abstract long Rd64(long coord);
        internal abstract void Wr64(long coord, long val);

    }
}
