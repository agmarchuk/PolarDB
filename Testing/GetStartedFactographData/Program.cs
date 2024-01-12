using System.Xml.Linq;
using System.IO;
using Factograph.Data;
using Factograph.Data.r;
using System.Xml.Serialization;

partial class Program
{
    /// <summary>
    /// Программа осуществляет присоединение к фактографической базе данных и обеспечвает
    /// выполнение методов доступа. 
    /// Сервису данных подается директория (path должен заканчиваться /) в которой должны
    /// находится два файла: config.xml и онтология Ontology_iis-v14.xml
    /// В файле конфигуратора config.xml имеется connectionstring с указанием протокола upi 
    /// (единственный вариант) и папки для базы данных:
    ///     <database connectionstring="upi:D:\Home\data\upi\"/>
    /// Перед запуском программы папка должна быть создана и очищена!
    /// Кроме того, в конфигураторе перечисляются используемые кассеты, напр:
    ///     <LoadCassette>D:\Home\FactographProjects\syp_cassettes\SypCassete</LoadCassette>
    /// Перед запуском, кассеты должны раполагаться на указанных местах!
    /// </summary>
    public static void Main()
    {
        Console.WriteLine("Start FactographData use sample."); 
        string wwwpath = "../../../wwwroot/"; // Это для запуска через dotnet
        Factograph.Data.IFDataService db = new Factograph.Data.FDataService(wwwpath, wwwpath + "Ontology_iis-v14.xml", null);
        db.Reload(); // Это действие необходимо если меняется набор кассет

        // Определим процедуру вывода потока записей на консоль в формате N3
        // Запись состоит из идентификатора, типа и набора свойств
        Action<RRecord> printRRecord = (record) =>
        {
            Console.WriteLine($"<{record.Id}> rdf:type <{record.Tp}> ;");
            foreach (var pair in record.Props.Select((rprop, nom) => new { rprop, nom }))
            {
                RProperty rprop = pair.rprop;
                // У свойства есть предикат
                Console.Write($"\t<{rprop.Prop}> ");
                if (rprop is RField)
                { // Свойство может быть свойством данных - строковое значение и язык
                    RField rField = (RField)rprop;
                    Console.Write($"\"{rField.Value}\"");
                    if (rField.Lang != null) Console.Write($"^^{rField.Lang}");
                    Console.Write(" ");
                }
                else if (rprop is RLink)
                { // или Свойство может быть прямой ссылкой на объект
                    RLink rLink = (RLink)rprop;
                    Console.Write($"<{rLink.Resource}> ");
                }
                else if (rprop is RInverseLink)
                { // или Свойство может быть прямой ссылкой на объект
                    RInverseLink riLink = (RInverseLink)rprop;
                    Console.Write($"[[[<{riLink.Source}>]]] ");
                }
                Console.WriteLine(pair.nom == record.Props.Length - 1 ? "." : ";");
            }
        };

        // Поиск записей по имени
        IEnumerable<RRecord> records = db.SearchRRecords("марчук", false);

        // Посмотрим результат в формате N3
        foreach (var record in records) printRRecord(record);

        // Выведем результат поиска как набор гиперссылок
        foreach (var record in records)
        {
            Console.WriteLine($"<a href='{record.Id}'>{record.GetName()}</a>");
        }

        // Берем идентификатор первой записи, получаем расширенную запись
        string itemId = records.First().Id;

        var extendedRecord = db.GetRRecord(itemId, true);
        if (extendedRecord == null) throw new Exception($"Err: no item for {itemId}");
        Console.WriteLine("Extended record:");
        printRRecord(extendedRecord);

        // Переходим к модели r.Rec. Сначала вычисляем универсальный шаблон для данного типа
        var shablon = Rec.GetUniShablon(extendedRecord.Tp, 2, null, db.ontology);
        // Потом раскладываем ресширенную запись в соответствии с шаблоном
        var tree = Rec.Build(extendedRecord, shablon, db.ontology, idd => db.GetRRecord(idd, false));

        // Попробуем разложить полученное дерево по элементам
        // Сначала заголовок
        Console.WriteLine($"{tree.Id} {tree.Tp}");
        // Потом идут свойства, будем их раскрывать в цикле, а значения помещать в таблицу
        Console.WriteLine("<table>");
        foreach (var prop in tree.Props)
        {
            // У свойства есть имя предиката и некоторое значение, разное, в зависимости от класса
            // в таблице создадим две колонки, первая - предикат
            string pred = prop.Pred;
            // Вторая колонка будет зависеть от типа свойства
            string second_col = "";
            if (prop is Tex)
            {
                Tex t = (Tex)prop;
                if (t.Values.Length > 0)
                    second_col = t.Values.Select(tl => tl.Text + "^^" + tl.Lang)
                        .Aggregate((sum, s) => sum + ", " + s);
            }
            else if (prop is Str)
            {
                Str s = (Str)prop;
                second_col = s.Value ?? "";
            }
            else if (prop is Dir)
            {
                Dir d = (Dir)prop;
                if (d.Resources.Length > 0)
                {
                    second_col = "<table>";
                    foreach (var r in d.Resources)
                    {
                        second_col += $"<tr><td><a href='{r?.Id}'>{r?.GetText("http://fogid.net/o/name")}</a></td></tr>";
                    }
                    second_col += "</table>";
                }
            }
            else if (prop is Inv)
            {
                Inv inv = (Inv)prop;
                if (inv.Sources.Length > 0)
                {
                    second_col = "<table>";
                    foreach (var r in inv.Sources)
                    {
                        second_col += $"<tr><td><a href='{r.Id}'>ссылка</a></td></tr>\n";
                    }
                    second_col += "</table>";
                }
            }

            if (second_col.Length > 0)
                Console.WriteLine($"<tr><td>{pred}</td><td>{second_col}</td></tr>");
        }
        Console.WriteLine("</table>");

        // Модернизированный подход и представление:
        // Выделяем отдельную таблицу для визуализации, в ней по горизонтали располагаем поля и прямые ссылки
        // Таблицу реализуем средствами XElement
        Rec tt = tree;
        XElement table = new XElement("table");
        // Сначала добавим заголовки
        table.Add(new XElement("tr",
            tt.Props.Where(p => p is Tex || p is Str || p is Dir)
                .Select(p => new XElement("th", p.Pred))
            ));
        // Теперь добавляем рядок с данными
        table.Add(new XElement("tr",
            tt.Props.Where(p => p is Tex || p is Str || p is Dir)
                .Select(p =>
                {
                    string val = "value";
                    return new XElement("td", val);
                })
            ));
        Console.WriteLine(table.ToString());


    }


}