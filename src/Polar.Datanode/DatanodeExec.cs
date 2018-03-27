using System;
using System.Collections.Generic;
using System.Text;

namespace Polar.Datanode
{
    public partial class Datanode
    {
        // Константы теги
        private const int _none = 0;
        private const int _sendinttest = 1; // Тест посылки целого резерв
        private const int _clear = 2;
        private const int _initcommunications = 3; // временный вариант (былвакантный
        private const int _appendonlyrecord = 4;
        private const int _appendonlyextkey = 5;
        private const int _flush = 6;
        private const int _calculatestaticindex = 7;
        private const int _activate = 8;
        private const int _getbykey = 9;
        private const int _getallprimarybyexternal = 10;
        private const int _createdatabase = 11;
        private const int _saveconfiguration = 12;
        private const int _loadconfiguration = 13;
        private const int _activatedatabase = 14;
        private const int _setconfiguration = 15;


        private object ExecComm(int cm, int tab, object ob)
        {
            switch (cm)
            {
                case 0: { return null; }
                case _sendinttest: { int nn = (int)ob; return new object[] { 999, "pupkin_vasya", 13 }; }
                //case _clear: { Clear3(); return null; }
                //case _initcommunications: { int nn = (int)ob; InitNodes(nn); return null; }
                //case _appendonlyrecord: { AppendOnlyRecord(tab, (object[])((object[])ob)[0], (bool)((object[])ob)[1]); return null; }
                //case _appendonlyextkey: { AppendOnlyExtKey(tab, (int)((object[])ob)[0], (int)((object[])ob)[1], (int)((object[])ob)[2], (bool)((object[])ob)[3]); return null; }
                //case _flush: { Flush(); return null; }
                //case _calculatestaticindex: { CalculateStaticIndex(); return null; }
                //case _activate: { Activate(); return null; }
                //case _getbykey:
                //    {
                //        object rec = GetByKey(tab, (int)((object[])ob)[0]);
                //        return rec;
                //    }
                //case _getallprimarybyexternal:
                //    {
                //        var prims = GetAllPrimaryByExternal(tab, (int)((object[])ob)[0], (int)((object[])ob)[1]);
                //        return prims;
                //    }
                //case _createdatabase: { CreateDatabase(); return null; }
                //case _saveconfiguration: { SaveConfiguration(ob); return null; }
                //case _loadconfiguration: { LoadConfiguration(); return null; }
                //case _activatedatabase: { ActivateDatabase(); return null; }
                //case _setconfiguration:
                //    {
                //        SetConfiguration((object[])ob); return null;
                //    }

                default: throw new Exception($"Err: unknown command {cm}");
            }
        }

    }
}
