using System;
using System.Linq;


namespace Polar.DB
{
    public class Hashfunctions
    {
        // Процедура похоже не приспособлена к работе с юникодом и национальными алфавитами.
        public static int HashRot13(string str)
        {
            UInt32 hash = 0;
            foreach (char c in str)
            {
                //hash += Convert.ToByte(c);
                hash += Convert.ToUInt32(c) & 255; // Это я сделал из-за русского и др. языков
                hash -= (hash << 13) | (hash >> 19);
            }
            return (int)hash;
        }
        public static int First4charsRu(string s)
        {
            // Специальное кодирование. В принципе, все расположено почти по естественному порядку. Исключение - группа [\\]^_`
            // Сомнение вызывает наличие в таблице Ё. Кодирование оставляет неиспользованными 4 старших разряда кода.
            //string selected_chars = "!\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHJKLMNOPQRSTUWXYZ[\\]^_`{|}~АБВГДЕЖЗИЙКЛМНОПРСТУФКЦЧШЩЪЫЬЭЮЯЁ";
            const string schars = "!\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHJKLMNOPQRSTUWXYZ[\\]^_`{|}~АБВГДЕЖЗИЙКЛМНОПРСТУФКЦЧШЩЪЫЬЭЮЯЁ";
            int len = s.Length;
            var chs = s.ToCharArray()
                .Concat(Enumerable.Repeat(' ', len < 4 ? 4 - len : 0))
                .Take(4)
                .Select(ch =>
                {
                    int ind = schars.IndexOf(char.ToUpper(ch));
                    if (ind == -1) ind = 0; // неизвестный символ помечается как '!'
                    return ind;
                }).ToArray();
            return ((((((chs[0] << 7) | chs[1]) << 7) | chs[2]) << 7) | chs[3]);
        }

    }
}
