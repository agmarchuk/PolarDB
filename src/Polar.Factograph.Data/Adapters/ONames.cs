using System.Xml.Linq;

namespace Polar.Factograph.Data.Adapters
{
    public class ONames
    {
        public static XName rdfabout = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about";
        public static XName rdfresource = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource";
        public const string fogi = "{http://fogid.net/o/}";
        public const string fog = "http://fogid.net/o/";
        public static XName xmllang = "{http://www.w3.org/XML/1998/namespace}lang";

        public static XName ToXName(string xid)
        {
            int pos = xid.LastIndexOfAny(new char[] { '/', '#' });
            return XName.Get(xid.Substring(pos + 1), xid.Substring(0, pos + 1));
        }
    }
}