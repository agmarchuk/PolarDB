using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Polar.DB;
using Polar.Cells;
using Polar.PagedStreams;

namespace GetStarted
{
    //static string path = "Databases/"; // Это общее для модулей определение
    public partial class Program
    {
        // Проверка скорости ввода данных
        public static void Main9()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Start Program9");
            int mbytes = 800;
            int nelements = mbytes / 8 * 1024 * 1024;

            // ======== Проверяем ввод данных через IO =======
            //FileStream fs = new FileStream(path + "filestream.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 10000000);
            FileStream fs = new FileStream(dbpath + "filestream.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            byte[] buffer = new byte[2048 * 32];
            for (int i = 0; i< buffer.Length; i++) { buffer[i] = (byte)(i & 255); }
            sw.Restart();
            for (int i = 0; i < 16 * mbytes; i++)
            {
                fs.Write(buffer, 0, buffer.Length);
            }
            sw.Stop();
            Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for writing {mbytes} mbytes");

            // ======== Проверяем ввод данных через BinaryWriter =======
            fs.Position = 0L;
            BinaryWriter bw = new BinaryWriter(fs);
            long val = 29877777298392742L;
            sw.Restart();
            for (int i = 0; i< mbytes / 8 * 1024 * 1024; i++)
            {
                bw.Write(val);
            }
            sw.Stop();
            Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for writing {mbytes / 8 * 1024 * 1024} long numbers");

            // ======== Проверяем ввод данных через ячейку =======
            PaCell cell = new PaCell(new PTypeSequence(new PType(PTypeEnumeration.longinteger)), dbpath + "sequ_long.pac", false);
            cell.Clear();
            cell.Fill(new object[0]);
            sw.Restart();
            for (int i = 0; i < nelements; i++)
            {
                cell.Root.AppendElement((long)(nelements - i));
            }
            cell.Flush();
            sw.Stop();
            Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for writing {mbytes / 8 * 1024 * 1024} long elements");

            // ======== Проверяем ввод данных через ячейку =======
            PType tp_rec = new PTypeRecord(
                new NamedType("f1", new PType(PTypeEnumeration.integer)),
                new NamedType("f2", new PType(PTypeEnumeration.sstring)),
                new NamedType("f3", new PType(PTypeEnumeration.real)));
            PaCell cell2 = new PaCell(new PTypeSequence(tp_rec), dbpath + "sequ_rec.pac", false);
            cell2.Clear();
            cell2.Fill(new object[0]);
            sw.Restart();
            int ne = nelements / 3;
            for (int i = 0; i < ne; i++)
            {
                int id = nelements - i;
                cell2.Root.AppendElement(new object[] { id, "=" + id, 5.5 });
            }
            cell2.Flush();
            sw.Stop();
            Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for writing {ne} record elements");

            // ======== Проверяем ввод данных через ячейку =======
            PType tp_rec3 = new PTypeRecord(
                new NamedType("f1", new PType(PTypeEnumeration.integer)),
                new NamedType("f2", new PType(PTypeEnumeration.sstring)),
                new NamedType("f3", new PType(PTypeEnumeration.real)));
            Stream stream = new FileStream(dbpath + "fstream.bin",  FileMode.OpenOrCreate, FileAccess.ReadWrite); //new MemoryStream();
            BinaryWriter bw3 = new BinaryWriter(stream);
            sw.Restart();
            int ne3 = nelements / 3;
            for (int i = 0; i < ne3; i++)
            {
                int id = nelements - i;
                PaCell.SetPO(tp_rec3, bw3, new object[] { id, "=" + id, 5.5 });
            }

            sw.Stop();
            Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for writing {ne3} record elements to memory stream");

            // ======== Проверяем ввод данных через ячейку и страничное хранилище потоков =======
            PType tp_rec4 = new PTypeRecord(
                new NamedType("f1", new PType(PTypeEnumeration.integer)),
                new NamedType("f2", new PType(PTypeEnumeration.sstring)),
                new NamedType("f3", new PType(PTypeEnumeration.real)));

            //PagedStreamStore ps_store = new PagedStreamStore(path + "storage.bin", 4); // заказали 4 стрима, конкретные будут: ps_store[i]
            StreamStorage ps_store = new StreamStorage(dbpath + "storage9.bin", 4);
            ps_store.DeactivateCache();
            PaCell cell4 = new PaCell(new PTypeSequence(tp_rec), ps_store[0], false);
            cell4.Clear();
            cell4.Fill(new object[0]);
            sw.Restart();
            int ne4 = nelements / 3;
            for (int i = 0; i < ne4; i++)
            {
                int id = nelements - i;
                cell4.Root.AppendElement(new object[] { id, "=" + id, 5.5 });
            }
            cell4.Flush();
            sw.Stop();
            Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for storing {ne} record elements");

        }
        // ======================== Результаты ==========================
        // 248 ms. for writing 800 mbytes
        // 2026 ms. for writing 100 M long numbers (1881 ms если сильно увеличить буфер)
        // 7420 ms. for writing 100 M long elements to cell
        // 10-14 sec. for  830 MB (33 M records)

    }
}
