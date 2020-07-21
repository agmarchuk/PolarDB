using Polar.Common;
using Polar.DB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Samples
{
    public class Demo101 : ISample
    {
        public ICollection<IField> Fields { get
            {
                return new List<IField>() { 
                    new NumericField("Number of persons", "npersons") { DefaultValue = 1_000_000 }
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
            Console.WriteLine("Start Demo101");
            // === Демонстрация базовых действий со структурами ===
            // Создаем тип персоны
            PType tp_person = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.integer)),
                new NamedType("name", new PType(PTypeEnumeration.sstring)),
                new NamedType("age", new PType(PTypeEnumeration.integer)));
            // делаем персону в объектном представлении
            object ivanov = new object[] { 7001, "Иванов", 20 };
            // интерпретируем объект в контексте типа
            Console.WriteLine(tp_person.Interpret(ivanov, true));
            // то же, но без имен полей
            Console.WriteLine(tp_person.Interpret(ivanov));
            Console.WriteLine();

            // Создадим поток байтов. Это мог бы быть файл:
            MemoryStream mstream = new MemoryStream();
            // Поработаем через текстовый интерфейс
            TextWriter tw = new StreamWriter(mstream);
            TextFlow.Serialize(tw, ivanov, tp_person);
            tw.Flush();
            // Прочитаем то что записали
            TextReader tr = new StreamReader(mstream);
            mstream.Position = 0L;
            string instream = tr.ReadToEnd();
            Console.WriteLine($"======== instream={instream}");
            Console.WriteLine();

            // Теперь десериализуем
            ivanov = null;
            mstream.Position = 0L;
            ivanov = TextFlow.Deserialize(tr, tp_person);
            // проинтерпретируем объект и посмотрим
            Console.WriteLine(tp_person.Interpret(ivanov));
            Console.WriteLine();

            // ===== Последовательности =====
            // Создаем тип последовательности персон
            PType tp_persons = new PTypeSequence(tp_person);
            // Сделаем генератор персон
            Random rnd = new Random();
            Func<int, IEnumerable<object>> GenPers = nper => Enumerable.Range(0, nper)
                .Select(i => new object[] { i, "Иванов_" + i, rnd.Next(130) });

            // Сгенерируем пробу и проинтерпретируем
            object sequobj = GenPers(20).ToArray();
            Console.WriteLine(tp_persons.Interpret(sequobj));
            Console.WriteLine();

            // Чем плохо такое решение? Тем, что весь большой объект (последовательность записей) разворачивается в ОЗУ
            // Более экономным, как правило, является использование последовательностей

            string dbpath = System.IO.Path.GetTempPath();
            stream = new FileStream(dbpath + "db0.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            Stream filestream = stream;
           UniversalSequenceBase usequence = new UniversalSequenceBase(tp_person, filestream);

            // Последовательность можно очистить, в нее можно добавлять элементы, в конце добавлений нужно сбросить буфер
            usequence.Clear();
            foreach (object record in GenPers(npersons))
            {
                usequence.AppendElement(record);
            }
            usequence.Flush();

            // Теперь можно сканировать последовательность
            int totalages = 0;
            usequence.Scan((off, ob) => { totalages += (int)((object[])ob)[2]; return true; });
            Console.WriteLine($"total ages = {totalages}");
        }
        //END_SOURCE_CODE
        public string Name { get; set; }
        public string DiplayName { get => "Demo101"; }
        public int npersons;
    }
}
