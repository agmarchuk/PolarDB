using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace GetStarted
{
    partial class Program 
    {
        public static void Demo101()
        {
            Console.WriteLine("Start Demo101");
            // === Демонстрация базовых действий со структурами ===
            // Создаем тип персоны
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            // делаем персону в объектном представлении
            object[] ivanov = new object[] { 7001, "Иванов", 20 };
            // интерпретируем объект в контексте типа
            Console.WriteLine(tp_person.Interpret(ivanov, true));
            // то же, но без имен полей
            Console.WriteLine(tp_person.Interpret(ivanov));
            Console.WriteLine();

            // Создаем тип последовательности персон
            PType tp_persons = new PTypeSequence(tp_person);
            // Сделаем генератор персон
            Random rnd = new Random();
            Func<int, IEnumerable<object>> GenPers = nper => Enumerable.Range(0, nper)
                .Select(i => new object[] { i, "Иванов_" + i, rnd.Next(130) });

            // Сгенерируем пробу и проинтерпретируем
            object sequ = GenPers(20).ToArray();
            Console.WriteLine(tp_persons.Interpret(sequ));
            Console.WriteLine();

            // Создадим поток байтов. Это мог бы быть файл:
            MemoryStream mstream = new MemoryStream();
            // Поработаем через текстовый интерфейс
            TextWriter tw = new StreamWriter(mstream);
            TextFlow.Serialize(tw, sequ, tp_persons);
            tw.Flush();
            // Прочитаем то что записали
            TextReader tr = new StreamReader(mstream);
            mstream.Position = 0L;
            string instream = tr.ReadToEnd();
            Console.WriteLine($"======== instream={instream}");
            Console.WriteLine();

            // Теперь десериализуем
            sequ = null;
            mstream.Position = 0L;
            sequ = TextFlow.Deserialize(tr, tp_persons);
            // проинтерпретируем объект и посмотрим
            Console.WriteLine(tp_persons.Interpret(sequ));
            Console.WriteLine();


        }
    }
}
