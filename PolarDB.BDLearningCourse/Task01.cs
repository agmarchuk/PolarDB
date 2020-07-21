using Polar.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.LearningCourse
{
    public class Task01 : ISample
    {
        public ICollection<IField> Fields
        {
            get
            {
                return new List<IField>() {
                    new NumericField("Number of elements", "nelements") { DefaultValue = 100_000_000 },
                    new BooleanField("Need load database", "toload") { DefaultValue = true }
                };
            }
        }
        private Stream stream;
        public void Clear()
        {
            try
            {
                if (stream != null)
                {
                    stream.Close();
                }
            }
            catch { }
        }
        //START_SOURCE_CODE
        public void Run()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            Console.WriteLine("Hello World!");
            FileStream fs = File.Open("data.bin", FileMode.OpenOrCreate);
            stream = fs;
            BinaryWriter bw = new BinaryWriter(fs);
            BinaryReader br = new BinaryReader(fs);

            if (toload)
            {
                sw.Restart();
                for (long ii = 0L; ii < nelements; ii++) bw.Write(ii);
                fs.Flush();
                sw.Stop();
                Console.WriteLine($"load time {sw.ElapsedMilliseconds}");
            }
            else
            {
                fs.Position = 0L;
                sw.Restart();
                byte[] buffer = new byte[100000];
                int nblocks = (int)(nelements * 8 / buffer.Length);
                for (int i = 0; i < nblocks; i++) fs.Read(buffer, 0, buffer.Length);
                //for (long ii = 0L; ii < nelements; ii++) br.ReadInt64();
                sw.Stop();
                Console.WriteLine($"warm up time {sw.ElapsedMilliseconds}");
            }



            fs.Position = (nelements * 2 / 3) * 8;
            long v = br.ReadInt64();
            Console.WriteLine($"v = {v}");

            sw.Restart();
            Random rnd = new Random();
            long nreads = 100_000;
            for (long ii = 0; ii < nreads; ii++)
            {
                long ind = rnd.Next((int)nelements);
                fs.Position = ind * 8;
                long val = br.ReadInt64();
                if (val != ind) throw new Exception($"Err: ind={ind} val={val}");
            }
            sw.Stop();
            Console.WriteLine($"elapsed {sw.ElapsedMilliseconds} ms.");
        }
        //END_SOURCE_CODE
        public string Name { get; set; }
        public string DiplayName { get => "Task 01 - Hello Big Data"; }
        public long nelements;
        public bool toload;
    }
}
