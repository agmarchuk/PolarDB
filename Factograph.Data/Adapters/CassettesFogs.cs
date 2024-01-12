using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Factograph.Data.Adapters
{
    public class CassInfo
    {
        public string? name;
        public string? path;
        public string? owner;
        public bool writable = false;
    }
    public class FogInfo
    {
        public string vid = ".fog"; // .fogx (XML), .fogp (Polar), .fogj (JSON)
        //public CassInfo cassette;
        public string pth; // координата файла
        //public object fog = null;
        // разные представления для работы
        public XElement? fogx = null;
        public object? fogp = null;
        public string? owner;
        public string? prefix;
        public int counter; // -1 - отсутствует
        public bool writable;
    }
}
