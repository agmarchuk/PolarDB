using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.PagedStreams
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Start PagedFileStore");
            string path = "";
            string fname = path + "fob.bin";
            bool fob_exists = File.Exists(fname);
            FileStream fs = new FileStream(fname, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            FileOfBlocks fob = new FileOfBlocks(fs);
            Stream first_stream = fob.GetFirstAsStream();
            if (!fob_exists)
            {
                PagedStream.InitPagedStreamHead(first_stream, 8L, 0, PagedStream.HEAD_SIZE);
                fob.Flush();
            }

            PagedStream ps = new PagedStream(fob, fob.GetFirstAsStream(), 8L);
            //ps.Clear();
            Console.WriteLine("stream length={0} position={1}", ps.Length, ps.Position);

            bool towrite = false;
            bool toread = false;
            if (towrite)
            {
                DirectoryInfo dirin = new DirectoryInfo(@"D:\Home\FactographDatabases\testdir");
                BinaryWriter bw = new BinaryWriter(ps);
                ps.Position = 0L;
                foreach (FileInfo f in dirin.GetFiles())
                {
                    PaCell.SetPO(new PolarDB.PType(PolarDB.PTypeEnumeration.sstring), bw, f.Name);
                    Stream stream = f.OpenRead();
                    PaCell.SetPO(new PolarDB.PType(PolarDB.PTypeEnumeration.longinteger), bw, stream.Length);
                    stream.CopyTo(ps);
                    break;
                }
                ps.Flush();
            }
            else if (toread)
            {
                string dirout = @"D:\Home\FactographDatabases\testout\";
                BinaryReader br = new BinaryReader(ps);
                ps.Position = 0L;
                for (;;)
                {
                    if (ps.Position >= ps.Length) break;
                    string name = (string)PolarDB.PaCell.GetPO(new PolarDB.PType(PolarDB.PTypeEnumeration.sstring), br);
                    FileStream stream_out = new FileStream(dirout + name, FileMode.CreateNew, FileAccess.Write);
                    byte[] buff = new byte[1000];
                    long len = (long)PolarDB.PaCell.GetPO(new PolarDB.PType(PolarDB.PTypeEnumeration.longinteger), br);
                    while (len > 0)
                    {
                        int count = (int)System.Math.Min((long)buff.Length, len);
                        int n = ps.Read(buff, 0, count);
                        if (n != count) throw new Exception("Err: 2898782349");
                        stream_out.Write(buff, 0, count);
                        len -= count;
                    }
                    stream_out.Flush();
                    stream_out.Dispose();
                }
            }
            bool tomake = false;
            if (tomake)
            {
                long cell_shift = ps.Length;
                Console.WriteLine("начало ячейки: {0}", cell_shift);
                PagedStream.InitPagedStreamHead(ps,cell_shift, 0L, -1L);
                PagedStream ps_cell = new PagedStream(fob, ps, cell_shift);
                PType tp = new PTypeSequence(new PType(PTypeEnumeration.integer));
                PaCell cell = new PaCell(tp, ps_cell, false);
                cell.Fill(new object[] { 111, 222, 333, 444, 555, 666, 777, 888, 999 });
                object ob = cell.Root.Get();
                Console.WriteLine("ob={0}", tp.Interpret(ob));
            }
            bool toadd = true;
            if (toadd)
            {
                long cell_shift = 640;
                PagedStream ps_cell = new PagedStream(fob, ps, cell_shift);
                PType tp = new PTypeSequence(new PType(PTypeEnumeration.integer));
                PaCell cell = new PaCell(tp, ps_cell, false);
                for (int i = 0; i < 1000000; i++) cell.Root.AppendElement(99999);
                cell.Flush();

                Console.WriteLine("n elements={0}", cell.Root.Count());
            }
            bool tolook = false;
            if (tolook)
            {
                long cell_shift = 640;
                PagedStream ps_cell = new PagedStream(fob, ps, cell_shift);
                PType tp = new PTypeSequence(new PType(PTypeEnumeration.integer));
                PaCell cell = new PaCell(tp, ps_cell, false);
                object ob = cell.Root.Get();
                Console.WriteLine("ob={0}", tp.Interpret(ob));
            }
        }
    }
}
