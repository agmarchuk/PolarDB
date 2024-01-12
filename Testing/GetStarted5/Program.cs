using Polar.DB;
using Polar.Universal;

Console.WriteLine("Start Polar.DB tests");

// ============ Типы структур и значения с труктур в объектном представлении ===========
Console.WriteLine("=== Структуры и сериализация ===");
// Создадим типы записи и последовательности записей
PType tp1 = new PTypeRecord(
    new NamedType("name", new PType(PTypeEnumeration.sstring)),
    new NamedType("age", new PType(PTypeEnumeration.integer)));
PType tp2 = new PTypeSequence(tp1);

// Создадим структурные значения этих типов в объектном представлении
object val1 = new object[] { "Иванов", 22 };
object val2 = new object[]
{
    new object[] { "Иванов", 22 },
    new object[] { "Петров", 33 },
    new object[] { "Сидоров", 44 }
};

// Визуализация структур в объектном представлении
Console.WriteLine(tp1.Interpret(val1));
Console.WriteLine(tp2.Interpret(val2));

// ============== Сериализация/Десериализация =============
// Сериализация выполняет отображение структуры на поток символов (текстовая сериализация) или  
// поток байтов (бинарная сериализация). Десериализация выполняет обратное преобразование.
Stream mstream = new MemoryStream();
// сериализация делается через текстовый райтер 
TextWriter tw = new StreamWriter(mstream);
TextFlow.Serialize(tw, val2, tp2);
tw.Flush();
// посмотрим что записалось
mstream.Position = 0L;
TextReader tr = new StreamReader(mstream);
string sss = tr.ReadToEnd();
Console.WriteLine("Накопилось в стриме: " + sss);

// десериализаця делатеся через текстовый ридер
mstream.Position = 0L;
object val = TextFlow.Deserialize(tr, tp2);
// Теперь надо посмотреть что в объекте
Console.WriteLine("После цикла сериализация/десериализация: " + tp2.Interpret(val));

// Бинарная сериализация упаковывает структуры в подряд идущие байты по принципу: 
// bool - 1 байт
// byte - 1 байт
// int - 4 байта
// long, double - 8 байтов
// строка - набор байтов определяемый BinaryWriter.Write((string)s)
// запись - подряд стоящие сериализации полей записи
// последовательность - long длина последовательности, подряд стоящие развертки элементов
// 
// Бинарная сериализация совместима с BinaryWriter и BinaryReader
// Конкртеные тест отсутствует

System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

// ======== Универсальная последовательность ==========
Console.WriteLine("// ======== Универсальная последовательность UniversalSequenceBase ==========\r\n");

// Создадим типы записи и последовательности записей
PType tp_record = new PTypeRecord(
    new NamedType("id", new PType(PTypeEnumeration.integer)),
    new NamedType("name", new PType(PTypeEnumeration.sstring)),
    new NamedType("age", new PType(PTypeEnumeration.integer)));

Stream stream = File.Open(@"D:\Home\data\data303.bin", FileMode.OpenOrCreate);
UniversalSequenceBase sequence = new UniversalSequenceBase(tp_record, stream);

Random rnd = new Random();
int nelements = 10_000_000;

// При заполнении массива, сохраним офсеты элементов в массиве
long[] offsets = new long[nelements];
int[] keys = new int[nelements];

bool toload = true;

if (toload)
{
    sw.Restart();
    sequence.Clear();
    for (int i = 0; i < nelements; i++)
    {
        int key = nelements - i - 1;
        offsets[i] = sequence.AppendElement(new object[] { key, "Иванов" + key, rnd.Next(1, 110) });
        keys[i] = key;
    }
    // отсортируем пару массивов keys, offsets по ключам
    Array.Sort(keys, offsets);
    sw.Stop();
    Console.WriteLine($"Load of {nelements} elements. duration={sw.ElapsedMilliseconds}");
}
else
{
    int ind = 0;
    sequence.Scan((off, obj) =>
    {
        offsets[ind] = off;
        keys[ind] = (int)((object[])obj)[0];
        ind++;
        return true;
    });
    // отсортируем пару массивов keys, offsets по ключам
    Array.Sort(keys, offsets);
}

// Будем делать выборку элементов по ключу
sw.Restart();
int ntests = 1000;
for (int j = 0; j < ntests; j++)
{
    int key = rnd.Next(nelements);
    int nom = Array.BinarySearch(keys, key);
    long off = offsets[nom];
    object[] fields = (object[])sequence.GetElement(off);
    if (key != (int)fields[0]) throw new Exception("1233eddf");
    //Console.WriteLine($"key={key} {fields[0]} {fields[1]} {fields[2]}");
}
sw.Stop();
Console.WriteLine($"duration of {ntests} tests is {sw.ElapsedMilliseconds} ms.");

// Результаты прогонов
// Домшний desktop i3, 8 Gb RAM
// 1 млн. записей. Загрузка 0.4 сек. 1000 тестов 6.8 мс.
// 10 млн. записей. Загрузка 3.6 сек. 1000 тестов 7.5 мс.
// 20 млн. записей. Загрузка 7.3 сек. 1000 тестов 7.7 мс.
// 50 млн. записей. Загрузка 19 сек. 1000 тестов 5.2 с.
// 100 млн. записей. Загрузка 42 сек. 1000 тестов 10.8 с.

// ======== Универсальная последовательность ==========
Console.WriteLine("==== Универсальная последовательность USequence (годится для редактирования) ====");
string dbfolder = @"D:\Home\data\main3\";

// Создадим типы записи и последовательности записей
PType tp_rec = new PTypeRecord(
    new NamedType("id", new PType(PTypeEnumeration.integer)),
    new NamedType("deleted", new PType(PTypeEnumeration.boolean)),
    new NamedType("name", new PType(PTypeEnumeration.sstring)),
    new NamedType("age", new PType(PTypeEnumeration.integer)));

// Функция "нуля"        
Func<object, bool> Isnull = ob => (bool)((object[])ob)[1]; // Второе поле может "обнулить" значение с этим id

// Генератор стримов
int file_no = 0;
Func<Stream> GenStream = () =>
    new FileStream(dbfolder + (file_no++) + ".bin",
        FileMode.OpenOrCreate, FileAccess.ReadWrite);

// Функция для ключа
Func<object, int> intId = obj => ((int)(((object[])obj)[0]));

// Создаем последовательность записей
USequence records = new USequence(tp_rec, GenStream,
    rec => (bool)((object[])rec)[1], // признак уничтоженности
    rec => (int)((object[])rec)[0], // как брать ключ
    hval => (int)hval, // как делать хеш от ключа
    true)
{
    StateFile = dbfolder + "state.bin",
};
// ====== Добавим дополнительные индексы 
// Заведем индекс для поля name (поле векторное, поэтому выдаем массив)
var names_ind = new SVectorIndex(GenStream, records,
    r => new string[] { (string)(((object[])r)[2]) });
// Заведем другой индекс - возраста
var ages_ind = new UVectorIndex(GenStream, records, new PType(PTypeEnumeration.integer),
    r => new IComparable[] { (int)(((object[])r)[3]) });
records.uindexes = new IUIndex[]
{
    names_ind, // В тесте можно две строчки закоментарить, тогда индексы строиться не будут
    ages_ind
};

nelements = 1_000_000;

toload = true;

if (toload)
{
    sw.Restart();
    int n = nelements;
    int max_age = nelements / 20;
    IEnumerable<object> flow = Enumerable.Range(0, nelements)
        .Select(i => new object[]
        {
            n-i-1,
            false,
            "Иванов" + (n-i-1),
            rnd.Next(0, max_age)
        });
    records.Clear();
    records.Load(flow);
    records.Build();
    sw.Stop();
    Console.WriteLine($"Load of {nelements} elements. duration={sw.ElapsedMilliseconds}");
}
else
{
    records.Refresh();
}

// Будем делать выборку элементов по ключу
sw.Restart();

//int key = nelements * 2 / 3;
//var r = records.GetByKey(key);
//Console.WriteLine(tp_rec.Interpret(r));

// Проверка производительности выборки по ключу
ntests = 1000;
for (int j = 0; j < ntests; j++)
{
    int key = rnd.Next(nelements);
    var r = records.GetByKey(key);
    if (key != (int)((object[])r)[0]) throw new Exception("1233eddf");
    //Console.WriteLine($"key={key} {fields[0]} {fields[1]} {fields[2]}");
}
sw.Stop();
Console.WriteLine($"GetByKey duration of {ntests} tests is {sw.ElapsedMilliseconds} ms.");

if (records.uindexes.Length == 2)
{
    // Поиск по части имени (использует индекс names_ind)
    var results = records.GetAllByLike(0, "Иванов" + (nelements * 2 / 30)); // 0 - номер индекса
    foreach (var re in results)
    {
        Console.WriteLine(tp_rec.Interpret(re));
    }
    Console.WriteLine();
    // Поиск по возрасту (использует индекс ages_ind)
    var results2 = records.GetAllByValue(1, 99);
    foreach (var re in results2)
    {
        Console.WriteLine(tp_rec.Interpret(re));
    }
}

    

    // Результаты прогонов
    // Рабочий desktop i3, 16 Gb RAM
    // 1 млн. записей. Загрузка 4.9 (1.1 сек. без индексов) 1000 тестов 16 мс.
    // 10 млн. записей. Загрузка 35.8 (7.3 сек.) 1000 тестов 17 мс.
    // 100 млн. записей. Загрузка (68 сек.) 1000 тестов 16 мс.



