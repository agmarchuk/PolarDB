using Polar.DB;
using Polar.Universal;

partial class Program
{
    private static void Main3()
    {
        Console.WriteLine("Start Main3: Использование USequence");
        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

        // Создадим типы записи и последовательности записей
        PType tp_rec = new PTypeRecord(
            new NamedType("id", new PType(PTypeEnumeration.integer)),
            new NamedType("deleted", new PType(PTypeEnumeration.boolean)),
            new NamedType("name", new PType(PTypeEnumeration.sstring)),
            new NamedType("age", new PType(PTypeEnumeration.integer)));

        // ======== Универсальная последовательность ==========
        string dbfolder = @"D:\Home\data\main3\";
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
            names_ind,
            ages_ind
        };


        Random rnd = new Random();
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

        int ntests = 1000;
        for (int j = 0; j < ntests; j++)
        {
            int key = rnd.Next(nelements);
            var r = records.GetByKey(key);
            if (key != (int)((object[])r)[0]) throw new Exception("1233eddf");
            //Console.WriteLine($"key={key} {fields[0]} {fields[1]} {fields[2]}");
        }
        sw.Stop();
        Console.WriteLine($"duration of {ntests} tests is {sw.ElapsedMilliseconds} ms.");

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

    }

    // Результаты прогонов
    // Рабочий desktop i3, 16 Gb RAM
    // 1 млн. записей. Загрузка 4.9 (1.1 сек. без индексов) 1000 тестов 16 мс.
    // 10 млн. записей. Загрузка 35.8 (7.3 сек.) 1000 тестов 17 мс.
    // 100 млн. записей. Загрузка (68 сек.) 1000 тестов 16 мс.

}

