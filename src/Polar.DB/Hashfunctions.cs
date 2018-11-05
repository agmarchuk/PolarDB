using System;


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
    }
}
