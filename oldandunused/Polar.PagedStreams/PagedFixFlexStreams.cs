using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Polar.PagedStreams
{
    /// <summary>
    /// Класс конкретизациирует PagedStream и представляет последовательность стримов фиксированного размера и растущего размера  
    /// </summary>
    public class PagedFixFlexStreams : PagedStream
    {
        public PagedFixFlexStreams(SetOfBlocks sob, Stream bearing_stream, long basic_head_off) : base(sob, bearing_stream, basic_head_off)
        {
        }

        /// <summary>
        /// Добавляет стрим фиксированного размера
        /// </summary>
        /// <param name="stream"></param>
        /// <returns>offset в основном PagedStream потоке</returns>
        public long AppendFixStream(Stream stream) { return -1L; }

        /// <summary>
        /// Добавляет стрим нефиксированного размера, напр. нулевого
        /// </summary>
        /// <param name="stream"></param>
        /// <returns>offset в основном PagedStream потоке</returns>
        public long AppendFlexStream(Stream stream) { return -1L; }
    }
}
