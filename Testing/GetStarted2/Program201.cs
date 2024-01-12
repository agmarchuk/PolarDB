using System;
using System.IO;
using Polar.DB;

namespace GetStarted2
{
    partial class Program
    {
        static void Main201()
        {
            Console.WriteLine("Start Program201"); 
            // Определение поляровских типов
            PType tp_rec = new PTypeRecord(
                new NamedType("имя", new PType(PTypeEnumeration.sstring)),
                new NamedType("возраст", new PType(PTypeEnumeration.integer)),
                new NamedType("мужчина", new PType(PTypeEnumeration.boolean)));
            PType tp_seq = new PTypeSequence(tp_rec);
            // Задание поляровского объекта и его визуализация
            object seq_value = new object[]
            {
                new object[] { "Иванов", 24, true },
                new object[] { "Петрова", 18, false },
                new object[] { "Пупкин", 22, true }
            };
            Console.WriteLine(tp_seq.Interpret(seq_value));

            // Текстовая сериализация
            // Создадим поток байтов. Это мог бы быть файл, но сделали в памяти
            MemoryStream mstream = new MemoryStream();
            // Поработаем через текстовый интерфейс
            TextWriter tw = new StreamWriter(mstream);
            TextFlow.Serialize(tw, seq_value, tp_seq);
            tw.Flush();
            // Прочитаем то что записали
            TextReader tr = new StreamReader(mstream);
            mstream.Position = 0L;
            string instream = tr.ReadToEnd();
            Console.WriteLine($"======== instream={instream}");
            Console.WriteLine();

            // Теперь десериализуем
            object db = null;
            mstream.Position = 0L;
            db = TextFlow.Deserialize(tr, tp_seq);
            // проинтерпретируем объект и посмотрим
            Console.WriteLine(tp_seq.Interpret(db));
            Console.WriteLine();


        }
    }
}
