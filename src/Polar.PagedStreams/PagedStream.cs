using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using Polar.DB;
using Polar.Cells;

namespace Polar.PagedStreams
{
    /// <summary>
    /// Страничный поток физически состоит из головы, непосредственно за ней следующего begin-блока и упорядоченного 
    /// массива блоков, формируемого 13-ю длиными целыми - десткриптором цепочки блоков, используемой для 
    /// формирования потока. Голова и begin-блок расположены в опорном потоке bearing_stream
    /// </summary>
    public class PagedStream : Stream
    {
        // Счетчик для отладки и измерений
        public static long counter = 0L;
        /// <summary>
        /// Множество блоков из которого все конструируется, множество формирует пространство с координатной системой 
        /// на основе длинных целых, которые кодируют номер блока и смещение в блоке
        /// </summary>
        private SetOfBlocks sob;

        /// <summary>
        /// Опорный поток
        /// </summary>
        Stream bearing_stream;
        /// <summary>
        /// начало головы в опорном потоке
        /// </summary>
        long bearing_head_offset;
        /// <summary>
        /// длина begin-блока
        /// </summary>
        long beginblock_length;
        
        /// <summary>
        /// Формируется поток байтов длиной stream_length, указатель (offset) в потоке stream_position.
        /// </summary>
        private long stream_length;
        /// <summary>
        /// Количество использованных блоков, вовлеченных в построение потока через десткриптор  
        /// </summary>
        private long n_blocks;
        /// <summary>
        /// Дескриптор описывает набор используемых блоков следующим образом. 
        /// Элементы массива десткриптора - координаты в пространстве SetOfBlocks. Первые 10 элементов указывают
        /// на координаты первых 10 блоков цепочки блоков. Причем нулевой блок может быть неполным, т.е. у него 
        /// может быть положительное смещение в указанном блоке. Остальные координаты, прямо или косвенно имеющиеся
        /// в дескрипторе, обязаны быть указаны с нулевым смещением. Отсутствие указателя в координате изображается -1.
        /// descriptor[10] - блок, содержащий sob.BlockSize / 8 указателей на следующие блоки. 
        /// </summary>
        private long[] descriptor = new long[13];

        private long stream_position; // Это значение не сохраняется в потоке
        private long krefsinblock; // BlockSize / 8 

        // ===== кеш индексных страниц ====
        //long currentindexpage_coord = -2L;
        long[] currentindexpage_array = null;
        long[][] currentrefref_array = null; 

        /// <summary>
        /// Вычисляет координату в пространстве SOB точки, заданной линейным смещением в формируемом потоке.
        /// Метод применим только для смещений, больших или равных beginblock_length (та часть потока доступна 
        /// через другие средства, т.е. через опорный поток)
        /// </summary>
        /// <param name="offset">смещение в потоке</param>
        /// <returns></returns>
        private long CoordInSOB(long offset)
        {
            long off = offset - beginblock_length; // Смещение без начальной части

            if (descriptor[0] == -1)
            {
                descriptor[0] = sob.GetNext();
            }
            long fromstartborder = off + sob.LocalPosition(descriptor[0]);
            long page_index = fromstartborder / sob.BlockSize;
            long offsetinpage = fromstartborder % sob.BlockSize;
            // Если page_index == 0, то бедет descriptor[0] + off
            if (page_index == 0) return descriptor[0] + off;
            // Если page_index < 10, то значение блока берется из ячейки десткриптора, а смещение будет остатком от деления
            if (page_index < 10)
            {
                if (descriptor[page_index] == -1)
                {
                    descriptor[page_index] = sob.GetNext();
                }
                return descriptor[page_index] + offsetinpage;
            }
            else if (page_index < 10 + krefsinblock)
            {
                if (descriptor[10] == -1)
                {
                    long page_new = sob.GetNext();
                    descriptor[10] = page_new;
                    for (int i = 0; i < krefsinblock; i++)
                    {
                        sob.Wr64(page_new + i * 8, -1);
                    }
                    // заполнение кеша
                    currentindexpage_array = Enumerable.Repeat<long>(-1L, (int)krefsinblock).ToArray();
                }
                else if (currentindexpage_array == null)
                { // Чтение из индексной страницы
                    currentindexpage_array = Enumerable.Repeat<long>(-1L, (int)krefsinblock).ToArray();
                    for (int i = 0; i < krefsinblock; i++)
                    {
                        long val = sob.Rd64(descriptor[10] + i * 8);
                        if (val == -1L) break;
                        currentindexpage_array[i] = val;
                    }
                }
                long pi = page_index - 10;
                long refpage = descriptor[10];
                long coordinpage = refpage + pi * 8;
                long blockfound = currentindexpage_array[pi]; // индексный кеш! было: // = sob.Rd64(coordinpage);
                if (blockfound == -1)
                {
                    long bf = sob.GetNext();
                    sob.Wr64(coordinpage, bf);
                    blockfound = bf;
                    // заполнение кеша
                    currentindexpage_array[pi] = bf;
                }
                return blockfound + offsetinpage;
            }
            else if (page_index < 10 + krefsinblock + krefsinblock * krefsinblock)
            {
                if (descriptor[11] == -1)
                {
                    long page_new = sob.GetNext();
                    descriptor[11] = page_new;
                    for (int i = 0; i < krefsinblock; i++)
                    {
                        sob.Wr64(page_new + i * 8, -1);
                    }
                    // заполнение кеша
                    currentrefref_array = Enumerable.Repeat<long[]>(null, (int)krefsinblock).ToArray();
                }
                else if (currentrefref_array == null)
                {
                    // заполнение кеша
                    currentrefref_array = Enumerable.Repeat<long[]>(null, (int)krefsinblock).ToArray();
                }
                long pi = page_index - 10 - krefsinblock;
                long refrefpage = descriptor[11];
                int ind = (int)(pi / krefsinblock);

                long refpage = -1;

                if (currentrefref_array[ind] == null)
                {
                    currentrefref_array[ind] = Enumerable.Repeat<long>(-1L, (int)krefsinblock).ToArray();
                }


                long blockfound = Int64.MinValue;
                long blockfound2 = currentrefref_array[ind][pi % krefsinblock];
                if (blockfound2 == -1L)
                {
                    long coordinrefrefpage = -1;
                    {
                        coordinrefrefpage = refrefpage + ind * 8;
                        refpage = sob.Rd64(coordinrefrefpage);
                        counter++;
                    }
                    if (refpage == -1)
                    {
                        long rp = sob.GetNext();
                        sob.Wr64(coordinrefrefpage, rp);
                        refpage = rp;
                        for (int i = 0; i < krefsinblock; i++)
                        {
                            sob.Wr64(rp + i * 8, -1);
                        }
                    }
                    long coordinpage = refpage + (pi % krefsinblock) * 8;
                    blockfound = sob.Rd64(coordinpage);
                    counter++;
                    if (blockfound == -1)
                    {
                        long bf = sob.GetNext();
                        sob.Wr64(coordinpage, bf);
                        blockfound = bf;
                    }

                    currentrefref_array[ind][pi % krefsinblock] = blockfound;
                }
                else
                {
                    //if (blockfound2 != blockfound) throw new Exception("Br-Br-Brrr!");
                    blockfound = blockfound2;
                }

                return blockfound + offsetinpage;
            }

            throw new Exception("Err: indirection of range > 2 is not implemented");
        }

        /// <summary>
        /// Страничный поток создается "на теле" страничной системы. Генератору пространство реализации потоков sob,
        /// (опорный) поток с установленным началом головы и следующего за ней информационного блока и длина головы и
        /// информационного блока
        /// </summary>
        public PagedStream(SetOfBlocks sob, Stream bearing_stream, long basic_head_offset)
        {
            this.sob = sob;
            this.bearing_stream = bearing_stream;
            this.bearing_head_offset = basic_head_offset;
            this.krefsinblock = sob.BlockSize / 8;
            // Чтение головы
            BinaryReader br = new BinaryReader(bearing_stream);
            bearing_stream.Position = bearing_head_offset;
            this.stream_length = br.ReadInt64();
            this.n_blocks = br.ReadInt64();
            for (int i = 0; i < 13; i++) descriptor[i] = br.ReadInt64();
            this.beginblock_length = br.ReadInt64();
            this.stream_position = 0L;

        }
        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return true; } }
        public override bool CanWrite { get { return true; } }

        public override long Length { get { return stream_length; } }

        public override long Position
        {
            get { return stream_position; }
            set
            {
                stream_position = value;
                if (stream_position < 0) throw new Exception("Err: position can't be negative");
                if (stream_position > stream_length) throw new Exception("Err: position>length in set Position property");
            }
        }

        public override void Flush()
        {
            BinaryWriter bw = new BinaryWriter(bearing_stream);
            bearing_stream.Position = bearing_head_offset;
            bw.Write(stream_length);
            bw.Write(n_blocks);
            for (int i = 0; i < 13; i++) bw.Write(descriptor[i]);
            bw.Write(beginblock_length);
            sob.Flush();
        }
        public void Clear()
        {
            stream_length = 0L;
            stream_position = 0L;
            Flush();
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            // Проверка невыхода за диапазон по чтению -- Это делать не нужно, в буфере может быть указан полный размер
            //if (stream_position + count > stream_length) throw new Exception("Err: reading out of borders");
            // Корректируем длину чтения в соответствии с длиной потока
            if (stream_position + count > stream_length) count = (int)(stream_length - stream_position);


            int cnt = 0; // прочитано байтов
            // Является ли position позицией в информационном блоке?
            if (stream_position < beginblock_length)
            {
                // Тогда можно прочитать минимум из count или beginblock_length - stream_position байтов
                cnt = System.Math.Min(count, (int)(beginblock_length - stream_position));
                bearing_stream.Position = bearing_head_offset + HEAD_SIZE + stream_position;
                int num = bearing_stream.Read(buffer, offset, cnt);
                if (num != cnt) throw new Exception("Err: 303942");
                stream_position += cnt;
                offset += cnt;
            }
            // Теперь организуем цикл по цепочке блоков, очередной блок будем определять по текущему offset
            while (cnt != count)
            {
                long block_coord = CoordInSOB(stream_position);
                // находим минимум из сколько надо и сколько есть
                int c = System.Math.Min(count - cnt, (int)((sob.BlockIndex(block_coord) + 1) * sob.BlockSize - block_coord));
                sob.Rd(block_coord, buffer, offset, c);
                stream_position += c;
                offset += c;
                cnt += c;
            }
            return cnt;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            // Сначала проверки. Это или изменение или добавление, но не одновременно.\
            bool appending = false;
            if (stream_position == stream_length) appending = true;
            if (!appending && stream_position + count > stream_length)
            {
                //throw new Exception("Err: update/append mixture");
                // Разбиваем на 2 записи
                int rest = (int)(stream_length - stream_position);
                Write(buffer, offset, rest);
                Write(buffer, offset + rest, count - rest);
                return;
            }

            int cnt = 0; // накапливаемое количество записанных байтов
            // Является ли position позицией в информационном блоке?
            if (stream_position < beginblock_length)
            {
                // Предполагается, что beginblock ТОЧНО есть и он непрерывный, поскольку он создается вместие с головой
                // Тогда можно записать минимум из count или beginblock_length - stream_position байтов
                cnt = System.Math.Min(count, (int)(beginblock_length - stream_position));
                bearing_stream.Position = bearing_head_offset + HEAD_SIZE + stream_position;
                bearing_stream.Write(buffer, offset, cnt);
                stream_position += cnt;
                offset += cnt;
            }
            // Теперь организуем цикл по цепочке блоков, очередной блок будем определять по текущему offset, который потихоньку увеличивается
            while (cnt != count)
            {
                long block_coord = CoordInSOB(stream_position);
                // находим минимум из сколько надо и сколько есть
                int c = System.Math.Min(count - cnt, (int)((sob.BlockIndex(block_coord) + 1) * sob.BlockSize - block_coord));
                sob.Wr(block_coord, buffer, offset, c);
                stream_position += c;
                offset += c;
                cnt += c;
            }
            //stream_position += count;
            if (appending) stream_length += count;
        }

        private static PType tp_first = null;
        private static PType TpHead
        {
            get
            {
                if (tp_first == null)
                {
                    tp_first = new PTypeRecord(
                        new NamedType("stream_length", new PType(PTypeEnumeration.longinteger)),
                        new NamedType("n_blocks", new PType(PTypeEnumeration.longinteger)),
                        new NamedType("D13", new PTypeRecord(
                            new NamedType("D0", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D1", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D2", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D3", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D4", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D5", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D6", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D7", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D8", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D9", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D10", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D11", new PType(PTypeEnumeration.longinteger)),
                            new NamedType("D12", new PType(PTypeEnumeration.longinteger)))),
                        new NamedType("beginblock_length", new PType(PTypeEnumeration.longinteger)));
                }
                return tp_first;

            }
        }

        /// <summary>
        /// длина головы = 3 + 13 длинных целых, это: 1 - для текущей длины, 1 - текущей позиции,
        /// 1 - количество блоков, определяемых дескриптором,
        /// 13 - дескриптора. Соответственно, эти информация в опорном 
        /// потоке реализована в байтах и расположена подряд. 
        /// </summary>
        public static long HEAD_SIZE { get { return (long)TpHead.HeadSize;  } }


        /// <summary>
        /// Создает голову нового потока в стриме с указанным смещением. Задаются также длина begin-блока и координата первого блока.  
        /// </summary>
        /// <param name="stream">Read/Write поток</param>
        /// <param name="offset_in_stream">Смещение в потоке</param>
        /// <param name="beginblock_length">длина begin-блока</param>
        /// <param name="_D0Coord">Координата первого (нулевого) блока или -1. Задание осмыслено только для самого начального потока в первом блоке</param>
        public static void InitPagedStreamHead(Stream stream, long offset_in_stream, long beginblock_length, long _D0Coord)
        {
            //BinaryReader br = new BinaryReader(stream);
            BinaryWriter bw = new BinaryWriter(stream);
            stream.Position = offset_in_stream;
            PaCell.SetPO(TpHead, bw, new object[] { 0L, _D0Coord==-1L? 0L : 1L, new object[] 
                { _D0Coord==-1L? -1L : _D0Coord + offset_in_stream, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L },
                  beginblock_length});
            bw.Flush();
        }
    }
}
