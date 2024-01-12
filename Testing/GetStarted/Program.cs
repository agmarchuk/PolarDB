using Polar.DB;
using Polar.Universal;

Console.WriteLine("GetStarted Start!");

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

string dbpath = @"D:/Home/data/GetStarted/";
Stream filestream = new FileStream(dbpath + "file0.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);
UniversalSequenceBase usequence = new UniversalSequenceBase(tp_person, filestream);

// Последовательность можно очистить, в нее можно добавлять элементы, в конце добавлений нужно сбросить буфер
int npersons = 1_000_000;
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

// Можно прочитать i-ый элемент
if (false)
{
    int ind = npersons * 2 / 3;
    object ores = usequence.GetByIndex(ind);
    Console.WriteLine($"element={tp_person.Interpret(ores)}");
    // Но нет - облом: Размер элемента не фиксирован (есть строка), к таким элементам по индексу обращаться не надо
}

// Чтобы организовать прямой доступ к элементам последовательности с нефиксированными размерами, нужен индекс
// Простейший индекс - массив офсетов
long[] offsets = new long[usequence.Count()];
int i = 0;
foreach (var pair in usequence.ElementOffsetValuePairs())
{
    offsets[i] = pair.Item1;
    i++;
}
// Теперь мы можем читать из последовательности элемент по номеру
int nom = npersons * 2 / 3;
long offset = offsets[nom];
object res = usequence.GetElement(offset);
Console.WriteLine($"element={tp_person.Interpret(res)}");

// Правильнее хранить индексный массив также в посделовательности
Stream filestream2 = new FileStream(dbpath + "file11.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);
UniversalSequenceBase offset_seq = new UniversalSequenceBase(
    new PType(PTypeEnumeration.longinteger), filestream2);
offset_seq.Clear();
foreach (var pair in usequence.ElementOffsetValuePairs())
{
    offset_seq.AppendElement(pair.Item1);
}
offset_seq.Flush();
// Теперь получение офсета еще проще
offset = (long)offset_seq.GetByIndex(nom);
// далее, как уже было
res = usequence.GetElement(offset);
Console.WriteLine($"element={tp_person.Interpret(res)}");

// ============= Универсальная последовательность =============
// Сигнатура конструктора:
//public USequence(PType tp_el, Func<Stream> streamGen, Func<object, bool> isEmpty,
//    Func<object, IComparable> keyFunc, Func<IComparable, int> hashOfKey, bool optimise = true);
// где: 
// tp_el - тип элементов последовательности
// streamGen - генератор стримов
// isEmpty - функция, определяющая что элемент пустой
// keyFunc - функция, дающая ключ (идентификатор) элемента
// hashOfKey - функция, задающая целочисленный хеш от ключа
int cnt = 0;
Func<Stream> GenStream = () => new System.IO.FileStream(dbpath + "f" + (cnt++) + ".bin",
    FileMode.OpenOrCreate, FileAccess.ReadWrite);
USequence usequ = new USequence(tp_person, dbpath + "statefile.bin", GenStream, ob => false, ob => (int)((object[])ob)[0], id => (int)id, false);
usequ.Load(GenPers(npersons));
usequ.Build();
var obj = usequ.GetByKey(nom);
Console.WriteLine($"element={tp_person.Interpret(obj)}"); // Мы получили требуемый элемент!

// =====================================================================
// ==================== Главная часть GetStarted =======================
// =====================================================================
// Типы структур и значения с труктур в объектном представлении ===========
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
Stream memstream = new MemoryStream();
// сериализация делается через текстовый райтер 
TextWriter textwriter = new StreamWriter(memstream);
TextFlow.Serialize(textwriter, val2, tp2);
textwriter.Flush();
// посмотрим что записалось
memstream.Position = 0L;
TextReader textreader = new StreamReader(memstream);
string sss = textreader.ReadToEnd();
Console.WriteLine("Накопилось в стриме: " + sss);

// десериализаця делатеся через текстовый ридер
memstream.Position = 0L;
object val = TextFlow.Deserialize(textreader, tp2);
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

// ======== Универсальная последовательность ==========
Console.WriteLine("==== Универсальная последовательность USequence (годится для редактирования) ====");

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
Func<Stream> GenStream2 = () =>
    new FileStream(dbpath + (file_no++) + ".bin",
        FileMode.OpenOrCreate, FileAccess.ReadWrite);

// Функция для ключа
Func<object, int> intId = obj => ((int)(((object[])obj)[0]));

// Создаем последовательность записей
USequence records = new USequence(tp_rec, dbpath + "state.bin", GenStream,
    rec => (bool)((object[])rec)[1], // признак уничтоженности
    rec => (int)((object[])rec)[0], // как брать ключ
    hval => (int)hval, // как делать хеш от ключа
    true);
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

int nelements = 1_000_000;

bool toload = true;

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
int ntests = 1000;
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

// ================= Редактирование записей в USequence ==================
// Редактирование включает в себя операции добавить, уничтожить, изменить запись. Идея редактирования заключается в том,
// что все операции редактирования сводятся к единственной - добавлению элемента. Если элемент имеет новый ключ, это
// добавление, если уже существующий, то это изменение записи на новую, если в добавляемом элементе есть признак
// isEmpty, то это уничтожение элемента с заданным ключом. Базовая операция редактирования:
// usequ.AppendElement(object element);
