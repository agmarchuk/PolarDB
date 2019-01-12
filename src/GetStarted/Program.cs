using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using Polar.DB;
using Polar.Cells;


namespace GetStarted
{
    public partial class Program
    {
        public static void Main(string[] args)
        {
            Main3();
            //Test(args);
        }
        public static void Main1()
        {
            Console.WriteLine("Start Main1");
            // Создадим тип записи из трех полей
            PType tp = new PTypeRecord(
                new NamedType("f1", new PType(PTypeEnumeration.integer)),
                new NamedType("f2", new PType(PTypeEnumeration.sstring)),
                new NamedType("f3", new PType(PTypeEnumeration.real)));
            // Объектное структурные значение может быть проинтерпретировано в виде текста
            Console.WriteLine(tp.Interpret(new object[] { 10, "=20=", 30.0009e-2 }, true));
            // Для хранилища нужен поток (Stream). Обычно используют файл, но можно любой поток (поток в ОЗУ).
            Stream stream = new MemoryStream();
            // Для хранения структурных значений используются ячейки. Заведем одну. 
            PaCell cell = new PaCell(tp, stream, false);
            // Заполним ячейку структрным значением, задав объектное представление.
            cell.Fill(new object[] { 777, "very very very loooooooooooooooooooong string", 3.141592 });
            // Из ячейки или из его поля можно взять структурное значение в объектном представлении
            object ob = cell.Root.Field(1).Get();
            Console.WriteLine(ob);
            //stream.Dispose();
        }
    }
}
