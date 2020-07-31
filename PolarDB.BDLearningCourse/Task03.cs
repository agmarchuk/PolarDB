using Polar.Cells;
using Polar.Common;
using Polar.DB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.LearningCourse
{
    public class Task03 : ISample
    {
        public ICollection<IField> Fields
        {
            get
            {
                return new List<IField>();
            }
        }
        private string dbpath;
        public void Clear()
        {
            try
            {
                if (Directory.Exists(dbpath))
                {
                    if (File.Exists(dbpath + "test.pac")) { File.Delete(dbpath + "test.pac"); }
                    if (File.Exists(dbpath + "test_rec.pac")) { File.Delete(dbpath + "test_rec.pac"); }
                    if (File.Exists(dbpath + "test_seq.pac")) { File.Delete(dbpath + "test_seq.pac"); }
                }
            }
            catch { }
        }
        //START_SOURCE_CODE
        public void Run()
        {
            Console.WriteLine("Start Task03_PolarDB");
            dbpath = System.IO.Path.GetTempPath();
            PType tp = new PType(PTypeEnumeration.sstring);
            PaCell cell = new PaCell(tp, dbpath + "test.pac", false);

            cell.Clear();
            cell.Fill("Привет из ячейки базы данных!");
            Console.WriteLine("Содержимое ячейки: {0}", cell.Root.Get());

            PType tp_rec = new PTypeRecord(
                new NamedType("имя", new PType(PTypeEnumeration.sstring)),
                new NamedType("возраст", new PType(PTypeEnumeration.integer)),
                new NamedType("мужчина", new PType(PTypeEnumeration.boolean)));
            object rec_value = new object[] { "Пупкин", 22, true };
            PaCell cell_rec = new PaCell(tp_rec, dbpath + "test_rec.pac", false);
            cell_rec.Clear();
            cell_rec.Fill(rec_value);
            object from_rec = cell_rec.Root.Get();
            Console.WriteLine(tp_rec.Interpret(from_rec));

            PType tp_seq = new PTypeSequence(tp_rec);
            object seq_value = new object[]
            {
                new object[] { "Иванов", 24, true },
                new object[] { "Петрова", 18, false },
                new object[] { "Пупкин", 22, true }
            };
            PaCell cell_seq = new PaCell(tp_seq, dbpath + "test_seq.pac", false);
            cell_seq.Clear();
            cell_seq.Fill(seq_value);
            object from_seq = cell_seq.Root.Get();
            Console.WriteLine(tp_seq.Interpret(from_seq));

            cell_seq.Root.AppendElement(new object[] { "Сидоров", 23, true });
            Console.WriteLine(tp_seq.Interpret(cell_seq.Root.Get()));

            long v0 = cell_seq.Root.Count();
            var v1 = cell_seq.Root.Element(2).Field(0).Get();
            var v2 = cell_seq.Root.Element(3).Field(1).Get();
            Console.WriteLine($"{v0} {v1} {v2}");

            cell_seq.Root.Element(1).Field(1).Set(19);
            cell_seq.Root.Element(1).Field(2).Set(true);
            Console.WriteLine(tp_seq.Interpret(cell_seq.Root.Get()));
            cell_seq.Flush();
            cell_seq.Close();
            cell_rec.Flush();
            cell_rec.Close();
            cell.Flush();
            cell.Close();
        }
        //END_SOURCE_CODE
        public string Name { get; set; }
        public string DiplayName { get => "Task03 - Начинаем осваивать Поляр"; }
    }
}
