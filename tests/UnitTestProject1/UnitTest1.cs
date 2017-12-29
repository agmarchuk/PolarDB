using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using Polar.DB;

namespace UnitTestProject1
{
    [TestClass]
    public class UnitTest1
    {
        PType tp_rec = new PTypeRecord(
            new NamedType("id", new PType(PTypeEnumeration.integer)),
            new NamedType("name", new PType(PTypeEnumeration.sstring)),
            new NamedType("age", new PType(PTypeEnumeration.real)));
        [TestMethod]
        public void TestPTypeInterpret()
        {
            object[] orec = new object[] { 777, "Pupkin", 9.9999 };
            string val = tp_rec.Interpret(orec);
            Assert.AreEqual(val, "{777,\"Pupkin\",9.9999}");
        }
        [TestMethod]
        public void TestPTypeToPObject()
        {
            object otype = tp_rec.ToPObject(3);
            string val = PType.TType.Interpret(otype);
            Assert.AreEqual(val, "record^[{\"id\",integer^},{\"name\",sstring^},{\"age\",real^}]");
        }
        [TestMethod]
        public void TestPTypeFromPObject()
        {
            object otype = tp_rec.ToPObject(3);
            PType tp = PType.FromPObject(otype);
            string val = tp.Interpret(new object[] { 777, "Pupkin", 9.9999 });
            Assert.AreEqual(val, "{777,\"Pupkin\",9.9999}");
        }
        [TestMethod]
        public void TestScale()
        {
            int[] arr1 = Enumerable.Range(0, 160).ToArray();
            var scale_fun = Scale.GetDiaFunc32(arr1);
            int index = 81;
            Diapason dia = scale_fun(index);
            Assert.IsTrue(dia.start <= index && dia.start + dia.numb > index , "" + index + " in " + dia.start + " " + dia.numb);
        }
        [TestMethod]
        public void TestTextFlowSerializeDeserialize()
        {
            MemoryStream stream = new MemoryStream();
            TextWriter tw = new StreamWriter(stream);
            TextFlow.Serialize(tw, new object[] { 777, "Pupkin", 9.9999 }, tp_rec);
            tw.Flush();

            byte[] bytes = stream.ToArray();
            string res = new string(bytes.Select(b => System.Convert.ToChar(b)).ToArray());
            Assert.AreEqual(res, "{777,\"Pupkin\",9.9999}");

            TextReader tr = new StreamReader(stream);
            stream.Position = 0L;
            object oval = TextFlow.Deserialize(tr, tp_rec);
            string val = tp_rec.Interpret(oval);
            Assert.AreEqual(val, "{777,\"Pupkin\",9.9999}");
        }
        [TestMethod]
        public void TestBinarySerialize()
        {
            MemoryStream mem = new MemoryStream();
            BinaryWriter bw = new BinaryWriter(mem);
            BinaryReader br = new BinaryReader(mem);

            ByteFlow.Serialize(bw, new object[] { 777, "Pupkin", 9.9999 }, tp_rec);
            bw.Flush();
            mem.Position = 0L;
            object oval = ByteFlow.Deserialize(br, tp_rec);
            string val = tp_rec.Interpret(oval);
            Assert.AreEqual(val, "{777,\"Pupkin\",9.9999}");
        }
    }
}
