using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    public partial class Program
    {
        public static void Test()
        {
            PType tp_point = new PTypeRecord(
                new NamedType("x", new PType(PTypeEnumeration.real)),
                new NamedType("y", new PType(PTypeEnumeration.real)));
            PType tp_figure = new PTypeUnion(
                new NamedType("nothing", new PType(PTypeEnumeration.none)),
                new NamedType("point", tp_point),
                new NamedType("polygon", new PTypeSequence(tp_point)),
                new NamedType("circle", new PTypeRecord(
                    new NamedType("center", tp_point),
                    new NamedType("radius", new PType(PTypeEnumeration.real)))));
            PType tp_sequ = new PTypeSequence(tp_figure);
            object sequ = new object[]
            {
                new object[] { 1, new object[] { 3.5, 7.8 }},
                new object[] { 2, new object[] {
                    new object[] { 0.0, 0.0 }, new object[] { 1.0, 0.0 }, new object[] { 1.0, 1.0 }, new object[] { 0.0, 1.0 } } },
                new object[] { 3, new object[] { new object[] { 5.0, 5.0 }, 4.99 } }
            };
            // Выполним текстовую сериализацию
            TextFlow.Serialize(Console.Out, sequ, tp_sequ);
            Console.WriteLine();
            // Создадим Stream, сделаем бинарную сериализацию, будем считать, что это файл
            Stream stream = new MemoryStream();
            ByteFlow.Serialize(new BinaryWriter(stream), sequ, tp_sequ);
            // Десериализуем стрим (бинарный файл)
            stream.Position = 0L;
            object sequ_1 = ByteFlow.Deserialize(new BinaryReader(stream), tp_sequ);
            // Проверим полученное значение
            Console.WriteLine(tp_sequ.Interpret(sequ_1));
        }
    }
}
