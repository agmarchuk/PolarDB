using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace GetStarted3
{
    partial class Program
    {
        public static void Main307()
        {
            int[] arr = new int[] { 2, 4, 5, 6, 7 };
            var query = Double(Enumerable.Range(0, 1000000));
            foreach (var r in query.Take(20))
            {
                Console.WriteLine(r);
            }
        }
        public static IEnumerable<int> Double(IEnumerable<int> flow)
        {
            //return flow.Select(x => x * 2);
            foreach (var e in flow)
            {
                yield return e * 2;
            }
        } 
    }
}
