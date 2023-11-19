using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Factograph.Data
{
    public interface IOntology
    {
        // === Интерфейс, реализованный в ROntology ===
        IEnumerable<string> GetAllClasses();
        RRecord[] OntoSpec { get; } // Онтологические спецификации 
        IEnumerable<string> AncestorsAndSelf(string id);
        IEnumerable<string> DescendantsAndSelf(string id);
        int PropsTotal(string tp);
        IEnumerable<string> GetInversePropsByType(string tp);
        IEnumerable<string> GetDirectPropsByType(string tp);
        string LabelOfOnto(string id);
        string LabelOfOnto(string id, string lang);
        string InvLabelOfOnto(string propId);
        IEnumerable<string> DomainsOfProp(string propId);
        IEnumerable<string> RangesOfProp(string propId);
        int PropPosition(string tp, string prop, bool isinverse);
        string EnumValue(string specificator, string stateval, string lang);
        bool IsEnumeration(string prop);
        KeyValuePair<string, string>[] EnumPairs(string prop, string lang);
        Dictionary<string, string[]> ParentsDictionary { get; }

        //RProperty[] ReorderFieldsDirects(RRecord record, string lang);
    }

}
