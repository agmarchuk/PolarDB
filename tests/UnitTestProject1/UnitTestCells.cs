using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using Polar.DB;
using Polar.Cells;

namespace UnitTestProject1
{
    [TestClass]
    public class UnitTestCells
    {
        private PType tp_rec;
        private Stream ss;
        private PaCell cell;
        public UnitTestCells()
        {
            tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.real)));
            ss = new MemoryStream();
            cell = new PaCell(tp_rec, ss, false);
        }

        [TestMethod]
        public void TestPaCellInit()
        {
            // чистим
            cell.Clear();
            // заполняем
            cell.Fill(new object[] { 7777, "Pupkin Vasya", 0.0001 });
            cell.Flush();
            // читаем
            object oval = cell.Root.Get();
            Assert.AreEqual(tp_rec.Interpret(oval), "{7777,\"Pupkin Vasya\",0.0001}");
            // читаем поле
            oval = cell.Root.Field(1).Get();
            Assert.IsTrue(oval is string);
            Assert.IsTrue((string)oval == "Pupkin Vasya");
            // читаем другое поле
            oval = cell.Root.Field(2).Get();
            Assert.IsTrue((double)oval == 0.0001);
            // Читаем десериализацией
            PaEntry ent = cell.Root.Field(0);
            long offset = ent.offset;
            Assert.IsTrue(offset == 32L, "offset: " + offset);
            BinaryReader br = new BinaryReader(ss);
            ss.Position = offset;
            object bval = ByteFlow.Deserialize(br, new PType(PTypeEnumeration.integer));
            Assert.IsTrue((int)bval == 7777, "" + (int)bval);
            // читаем запись
            ss.Position = offset;
            bval = ByteFlow.Deserialize(br, tp_rec);
            Assert.IsTrue(tp_rec.Interpret(bval) == "{7777,\"Pupkin Vasya\",0.0001}", tp_rec.Interpret(bval));

            // повторно, но с русским текстом
            cell.Clear();
            cell.Fill(new object[] { 7777, "Pupkin Вася", 0.0001 });
            ss.Position = offset;
            bval = ByteFlow.Deserialize(br, tp_rec);
            Assert.IsTrue(tp_rec.Interpret(bval) == "{7777,\"Pupkin Вася\",0.0001}", tp_rec.Interpret(bval));

            // теперь синтезирую значение ячейки
            cell.Clear();
            BinaryWriter bw = new BinaryWriter(ss);
            ss.Position = offset;
            bw.Write(7777);
            bw.Write("Pupkin Vasya");
            bw.Write((double)0.0001);
            bw.Flush();
            oval = cell.Root.Get();
            Assert.IsTrue(tp_rec.Interpret(oval) == "{7777,\"Pupkin Vasya\",0.0001}", tp_rec.Interpret(oval));

            // Теперь по-другому, через сериализацию
            cell.Clear();
            ss.Position = offset;
            ByteFlow.Serialize(bw, new object[] { 7777, "Pupkin Vasya", 0.0001 }, tp_rec);
            oval = cell.Root.Get();
            Assert.IsTrue(tp_rec.Interpret(oval) == "{7777,\"Pupkin Vasya\",0.0001}", tp_rec.Interpret(oval));
        }
        [TestMethod]
        public void TestSeqCell()
        {
            PType tp_seq = new PTypeSequence(new PType(PTypeEnumeration.integer));
            PaCell cell_seq = new PaCell(tp_seq, new MemoryStream(), false);
            cell_seq.Clear();
            cell_seq.Fill(new object[0]);
            cell_seq.Root.AppendElement(99);
            cell_seq.Root.AppendElement(98);
            cell_seq.Root.AppendElement(97);
            Assert.IsTrue(tp_seq.Interpret(cell_seq.Root.Get())== "[99,98,97]");
        }
    }
}
