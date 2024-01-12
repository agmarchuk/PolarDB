using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetStarted4
{
    partial class Program
    {
        public static void Main404flows()
        {
            Console.WriteLine("Start Main404flows");
            int npersons = 100;
            int ind = 0;
            IEnumerator source = new PeoplesSource();
            PeopleSink sink = new PeopleSink();
            
            while(source.MoveNext())
            {
                object[] rec = (object[])source.Current;
                //Console.WriteLine($"{rec[0]} {rec[1]} {rec[2]} ");
                if (ind >= npersons) break;
                ind++;
            }
        }
    }
    public class Transforms
    {
        public static object PeopleFilter(object ob)
        {
            return null;
        }
    }
    public class PeopleSink
    {
        int cnt = 0;
        public void Push(object val)
        {
            cnt++;
        }
        public int Count()
        {
            return cnt;
        }
    }
    public class PeoplesSource : IEnumerator
    {
        private int cnt = -1;
        public PeoplesSource()
        {

        }
        public object Current => new object[] { cnt, "p" + cnt, 21 };

        public bool MoveNext()
        {
            cnt++;
            return true;
        }

        public void Reset()
        {
            cnt = -1;
        }
        object IEnumerator.Current
        {
            get
            {
                return Current;
            }
        }
    }
}
