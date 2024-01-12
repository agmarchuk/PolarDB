using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Polar.PagedStreams
{
    public class FileOfBlocks : SetOfBlocks
    {
        // счетчик для отладки
        public static long counter = 0L;

        private FileStream fs;
        private BinaryReader br;
        private BinaryWriter bw;
        private long _block_length = 65536L; //16384L; //32768L; //65536L;

        private class CachePage { public byte[] array; public bool changed = false; }
        private List<CachePage> cache = null; // Если null, то не используется

        public FileOfBlocks(FileStream fs)
        {
            this.fs = fs;
            this.br = new BinaryReader(fs);
            this.bw = new BinaryWriter(fs);
            fs.Position = 0L;
            if (fs.Length == 0)
            {
                AddBlock();
                fs.Position = 0L;
                bw.Write(1L); //TODO: Эта ячейка уже не используется!
            }
            else
            {
            }
            ActivateCache();
        }
        public void Close() { this.fs.Dispose(); }
        public void ActivateCache()
        {
            cache = Enumerable.Repeat<CachePage>(null, (int)(fs.Length / _block_length)).ToList();
        }
        public void DeactivateCache() { cache = null; }
        public void LoadCache()
        {
            if (cache == null) ActivateCache();
            byte[] buffer = new byte[BlockSize];
            int bi = 0;
            fs.Position = 0L;
            for (long coord = 0L; coord < fs.Length; coord += BlockSize)
            {
                fs.Read(buffer, 0, (int)BlockSize);
                cache[bi] = new CachePage() { array = new byte[BlockSize] };
                Array.Copy(buffer, 0, cache[bi].array, 0, (int)BlockSize);
                bi++;
            }
        }

        public override long BlockSize { get { return _block_length; } }

        private void AddBlock()
        {
            long len = fs.Length;
            if (len < 0 || len % _block_length != 0) throw new Exception("Assert err: noncorrect block attributes in AddBlock()");
            fs.SetLength(len + _block_length);
            if (cache != null) cache.Add(null);
        }

        public override long GetFirst()
        {
            //if (n_blocks <= 0) throw new Exception("Assert err: n_blocks must be positive: " + n_blocks);
            return 0L;
        }

        public override Stream GetFirstAsStream()
        {
            return fs;
        }

        public override long GetNext()
        {
            long off = fs.Length; // n_blocks * _block_length;
            AddBlock();
            return off;
        }

        internal override int Rd(long coord, byte[] buffer, int buf_offset, int count)
        {
            int bi = (int)BlockIndex(coord);
            if (count < 0 || bi != BlockIndex(coord + count - 1)) throw new Exception("Err: 298311");
            int cnt = Int32.MinValue;
            //byte[] buffer2 = buffer.Select(b => b).ToArray();
            if (cache != null)
            {
                ActivateBlock(bi);
                int off = (int)LocalPosition(coord);
                Array.Copy(cache[bi].array, off, buffer, buf_offset, count);
                cnt = count;
            }
            else
            {
                if (coord != fs.Position) { fs.Position = coord; counter++; }
                cnt = fs.Read(buffer, buf_offset, count);
            }
            //for (int i = 0; i < buffer.Length; i++) if (buffer[i] != buffer2[i]) throw new Exception("lasjkd");
            return cnt;
            
        }

        private void ActivateBlock(int bi)
        {
            if (cache[bi] == null)
            {
                byte[] arr = new byte[_block_length];
                fs.Position = bi * BlockSize;
                fs.Read(arr, 0, arr.Length);
                cache[bi] = new CachePage() { array = arr };
            }
        }

        internal override long Rd64(long coord)
        {
            if (coord != fs.Position) { fs.Position = coord; counter++; }
            return br.ReadInt64();
        }

        internal override void Wr(long coord, byte[] buffer, int buf_offset, int count)
        {
            int bi = (int)BlockIndex(coord);
            if (count < 0 || bi != BlockIndex(coord + count - 1)) throw new Exception("Err: 298312");
            // === Кешевое решение
            if (cache != null)
            {
                ActivateBlock(bi);
                int off = (int)LocalPosition(coord);
                Array.Copy(buffer, buf_offset, cache[bi].array, off, count);
                cache[bi].changed = true;
            } // === конец.
            else
            {
            }
            if (coord != fs.Position) { fs.Position = coord; counter++; }
            fs.Write(buffer, buf_offset, count);
        }

        internal override void Wr64(long coord, long val)
        {
            if (coord != fs.Position) { fs.Position = coord; counter++; }
            bw.Write(val);
        }

        public void BigFlush()
        {
            if (false && cache != null)
            {
                int cnt = 0;
                for (int pi = 0; pi < cache.Count; pi++)
                {
                    CachePage cp = cache[pi];
                    if (cp == null || !cp.changed) continue;
                    fs.Position = (long)pi * BlockSize;
                    fs.Write(cp.array, 0, (int)BlockSize);
                    cp.changed = false;
                    cnt++;
                }
                Console.WriteLine("Flush cnt = {0}", cnt);
            }
            this.Flush();
        }
        public override void Flush()
        {
            fs.Flush();
        }

        public void Dump(long coord)
        {
            for (int i = 0; i<64; i++)
            {
                if (coord + i * 8 >= fs.Length) break;
                long val = Rd64(coord + i * 8);
                Console.Write("{0}\t", val);
                if ((i + 1) % 8 == 0) Console.WriteLine();
            }
        }
    }
}
