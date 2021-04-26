using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace GetStarted4
{
    partial class Program
    {
        public static void Main400()
        {
            Console.WriteLine("Start Main400");

            PType tp = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            object val1 = new object[] { 123, "Ivan", 21 };
            var s = tp.Interpret(val1);
            Console.WriteLine(s);
        }
    }
}
