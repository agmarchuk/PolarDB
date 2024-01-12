using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Polar.PagedStreams;

namespace UnitTestProject1
{
    [TestClass]
    public class UnitTestPagedstream
    {
        StreamStorage ss;
        public UnitTestPagedstream()
        {
            string fobname = "fob.bin";
            if (File.Exists(fobname)) File.Delete(fobname);
            ss = new StreamStorage(fobname);
        }
        [TestMethod]
        public void TestStreamStorage()
        {
            int n1, n2, n3;
            Stream s1, s2, s3;
            ss.CreateStream(out n1);
            ss.CreateStream(out n2);
            ss.CreateStream(out n3);
            s1 = ss[n1];
            s2 = ss[n2];
            s3 = ss[n3];
            string str = "This is long long long string constant";
            TextWriter tw = new StreamWriter(s1);
            for (int i=0; i<1000; i++)
            {
                tw.WriteLine(str);
            }
            BinaryWriter bw = new BinaryWriter(s2);
            for (int i=0; i< 1000000; i++)
            {
                bw.Write((long)1234567890);
            }
            for (int i = 0; i < 1000; i++)
            {
                tw.WriteLine(str);
            }
            tw.Flush();
            bw.Flush();

            long pos1 = s1.Position;

            BinaryReader br = new BinaryReader(s2); s2.Position = 0L;
            for (int i = 0; i < 1000000; i++)
            {
                long v = br.ReadInt64();
                Assert.IsTrue(v == (long)1234567890, $"v={v} i={i}");
            }
            TextReader tr = new StreamReader(s1); s1.Position = 0L;
            for (int i = 0; i < 2000; i++)
            {
                string str1 = tr.ReadLine();
                Assert.AreEqual(str, str1);
            }
            Assert.IsTrue(s1.Position == pos1);
            Assert.IsTrue(s2.Position == ((long)sizeof(long) * 1000000L));
            Assert.IsTrue(s3.Position == 0L);
            Assert.IsTrue(s3.Length == 0L);

        }
    }
}
