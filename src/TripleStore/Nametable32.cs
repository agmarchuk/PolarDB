using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Polar.DB;

namespace Polar.TripleStore
{
    /// <summary>
    /// Таблица имен с 32-разрядной базой кодирования и хеширования. Таблица сопоставляет строке целочисленный (32-разряда)
    /// код. Можно добавлять строки, можно по строке получать код, можно по коду получать строку.
    /// </summary>
    public class Nametable32
    {
        // Носителем таблицы является последовательность пар {код, строка}. Номер строки - ее код. Это первично. 
        // По коду строка определяется однозначно (как вводили), по строке код может определяться с учетом эквивалентностей.
        // Вначале таблица пустая, она заполняется 
        private UniversalSequenceBase cod_str;
        // Это офсеты основной таблицы. Номер элемента совпадает с кодом
        private UniversalSequenceBase offsets;
        // Индекс хеш строки - офсет. Индекс может быть неполный. Полный индекс - вместе со словарем 
        private IndexKey32CompImmutable index_str;
        // Динамическая часть индекса
        private Dictionary<string, int> dyna_index;

        public Nametable32(Func<Stream> stream_gen)
        {
            PType tp_elem = new PTypeRecord(
                new NamedType("code", new PType(PTypeEnumeration.integer)),
                new NamedType("str", new PType(PTypeEnumeration.sstring)));
            cod_str = new UniversalSequenceBase(tp_elem, stream_gen());
            offsets = new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), stream_gen());
            index_str = new IndexKey32CompImmutable(stream_gen, cod_str, ob => true,
                ob => Hashfunctions.HashRot13((string)((object[])ob)[1]), null);
            dyna_index = new Dictionary<string, int>();
        }
        public void Clear()
        {
            cod_str.Clear();
            offsets.Clear();
            index_str.Clear();
            dyna_index = new Dictionary<string, int>();
        }

        /// <summary>
        /// Перестраивает индекс index_str
        /// </summary>
        public void Build()
        {
            // Про порядок операторов еще надо подумать
            index_str.Build();
            dyna_index = new Dictionary<string, int>();
        }
        public void Refresh()
        {
            cod_str.Refresh();
            offsets.Refresh();
            index_str.Refresh();
        }
        // ==================== Динамика ===================

        /// <summary>
        /// Добавление НОВОГО имени, получение кода
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        private int SetStr(string s)
        {
            // Новый код определяется, записывается в основную таблицу, записывается в таблицу офсетов, записывается в динамический индекс
            int code = (int)cod_str.Count();
            long off = cod_str.AppendElement(new object[] { code, s });
            offsets.AppendElement(off);
            if (dyna_index.Count > 100_000_000)
            {
                System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                Flush();
                Build();
                sw.Stop();
                Console.WriteLine($"Build() {sw.ElapsedMilliseconds}");
            }
            else
            {
                dyna_index.Add(s, code);
            }
            // нужен итоговый Flush по двум последовательностям
            return code;
        }
        public void Flush()
        {
            cod_str.Flush();
            offsets.Flush();
            index_str.Flush();
        }
        public void Close()
        {
            //Flush();
            cod_str.Close();
            offsets.Close();
            index_str.Close();
        }
        public bool TryGetCode(string s, out int code)
        {
            if (dyna_index.TryGetValue(s, out code)) return true;
            code = -1;
            var q = index_str.GetAllBySample(new object[] { -1, s }).FirstOrDefault(ob => (string)((object[])ob)[1] == s);
            if (q == null) return false;
            code = (int)((object[])q)[0];
            return true;
        }
        public int GetSetStr(string s)
        {
            int code;
            if (TryGetCode(s, out code)) return code;
            code = SetStr(s);
            return code;
        }
        public string Decode(int cod)
        {
            long off = (long)offsets.GetByIndex(cod);
            return (string)((object[])cod_str.GetElement(off))[1];
        }
    }
}
