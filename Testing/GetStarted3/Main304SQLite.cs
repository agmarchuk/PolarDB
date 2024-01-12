using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SQLite;
using System.Data.Common;

namespace GetStarted3
{
    partial class Program
    {
        public static void Main304SQLite()
        {
            Console.WriteLine("Start Main304SQLite");

            string filename = datadirectory_path + "test304.db3";
            if (!System.IO.File.Exists(filename))
            {
                SQLiteConnection.CreateFile(filename);
            }

            DbProviderFactory factory = new SQLiteFactory();
            DbConnection connection = factory.CreateConnection();
            connection.ConnectionString = "Data Source=" + filename;

            DbCommand comm = connection.CreateCommand();
            DbTransaction transaction;

            int npersons = 400_000_000;


            bool toload = true;

            if (toload)
            {
                connection.Open();
                comm.CommandText = @"DROP TABLE persons;";
                try { comm.ExecuteNonQuery(); }
                catch (Exception ex) { Console.WriteLine($"Warning in DROP section {ex.Message}"); }
                connection.Close();

                connection.Open();
                comm.CommandText =
                @"CREATE TABLE persons (id INTEGER PRIMARY KEY ASC, name TEXT, age INTEGER);";
                try { comm.ExecuteNonQuery(); }
                catch (Exception ex) { Console.WriteLine($"Warning in CREATE TABLE section {ex.Message}"); }
                connection.Close();

                sw.Restart();
                connection.Open();
                transaction = connection.BeginTransaction();
                comm.Transaction = transaction;

                for (int i = 0; i < npersons; i++)
                {
                    int kwant = 1_000_000;
                    if (i % kwant == 0) Console.Write((i / kwant) + " ");
                    int k = npersons - i - 1;
                    comm.CommandText = "INSERT INTO persons VALUES (" + k + ",'" + k + "', 21);";
                    //Console.Write($"{comm.CommandText}");
                    comm.ExecuteNonQuery();
                }
                Console.WriteLine();
                transaction.Commit();
                connection.Close();
                sw.Stop();
                Console.WriteLine($"Loaded {npersons} elements in {sw.ElapsedMilliseconds} ms");
            }




            // Получение записи по ключу
            Random rnd = new Random();
            sw.Restart();

            connection.Open();
            transaction = connection.BeginTransaction();
            comm.Transaction = transaction;
            
            for (long i = 0; i < 1000; i += 1)
            {
                var com = connection.CreateCommand();
                //int key = (int)(npersons * 2 / 3);
                int key = rnd.Next((int)npersons);
                com.CommandText = "SELECT * FROM persons WHERE id=" + key + ";";
                object[] res = null;
                var reader = com.ExecuteReader();
                int cnt = 0;
                while (reader.Read())
                {
                    int ncols = reader.FieldCount;
                    res = new object[ncols];
                    for (int j = 0; j < ncols; j++) res[j] = reader.GetValue(j);
                    cnt += 1;
                }
                if (cnt == 0) { Console.WriteLine("no solutions. key = {key}"); }
                else if (cnt > 1) { Console.WriteLine("multiple solutions. key = {key} cnt = {cnt}"); }
                //Console.WriteLine($"{key} => {res[0]} {res[1]} {res[2]}");

                reader.Close();
            }

            transaction.Commit();
            
            connection.Close();
            sw.Stop();
            Console.WriteLine($"duration {sw.ElapsedMilliseconds}");
        }

        // Результаты (Desktop, Intel Core i3, RAM 8 Гб. :

        // Загрузка 100 тыс. 0.95 сек. 
        // Выборка 1 тыс. - 368 мс.    - раздельными коннекшинами
        // Выборка 1 тыс. - 117 мс.    - без объединения в транзакцию
        // Выборка 1 тыс. - 63 мс.     - в одной транзакции

        // Загрузка 1 млн. элементов 6 сек. 
        // Выборка 1 тыс. - 96 мс.     - в одной транзакции

        // Загрузка 10 млн. элементов 55 сек. (366 Мб)
        // Выборка 1 тыс. - 79 мс.     - в одной транзакции

        // Загрузка 100 млн. элементов 569 сек. (3913 Мб)
        // Выборка 1 тыс. - 9269 мс.     - в одной транзакции

        // Без загрузки
        // Выборка 1 тыс. - 9205 мс.     - в одной транзакции

        // ============ Рабочий комп. 5i, 16 Gb
        // 100_000 Load     1.4 s,    50 ms
        // 1_000_000 Load   9.4 s,    56 ms
        // 10_000_000 Load  85 s,     79 ms
        // 100_000_000 Load 874 s,    80 ms
        // 200_000_000 Load 1800 s,   68 ms
        // 400_000_000 Load 3672 s, 6641 ms
    }
}
