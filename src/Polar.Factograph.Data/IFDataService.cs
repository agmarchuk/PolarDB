using System.Xml.Linq;
//using Adapters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


namespace Polar.Factograph.Data
{
    public interface IFDataService
    {
        IOntology ontology { get; set; }
        void Init(string connectionstring);
        void Close();
        void Reload();
        // ============== Основные методы доступа к БД =============
        IEnumerable<XElement> SearchByName(string searchstring);
        IEnumerable<XElement> SearchByWords(string searchwords);
        XElement GetItemByIdBasic(string id, bool addinverse);
        XElement GetItemById(string id, XElement format);
        IEnumerable<XElement> GetAll();

        Polar.Factograph.Data.Adapters.DAdapter GetAdapter();

        // ============== Загрузка базы данных ===============
        //void StartFillDb(Action<string> turlog);
        //void LoadXFlow(IEnumerable<XElement> xflow, Dictionary<string, string> orig_ids);
        //void FinishFillDb(Action<string> turlog);

        // ============== Редктирование ===============
        XElement UpdateItem(XElement item);
        XElement PutItem(XElement item);

        // ============== Работа с файлами и кассетами ================
        string CassDirPath(string uri);
        string GetFilePath(string u, string s);
        string? GetOriginalPath(string u);
        bool HasWritabeFogForUser(string? user);
        Adapters.CassInfo[] Cassettes { get; }

        // ============ Билдеры =============
        TRecordBuilder TBuilder { get; }

        // ============ Работа с RRecord - могут (должны) быть переопределены ===========
        RRecord? GetRRecord(string? id, bool addinverse)
        {
            if (id == null) return null;
            XElement xrec = GetItemByIdBasic(id, addinverse);
            if (xrec != null && xrec.Attribute("id") != null && xrec.Attribute("type") != null)
            {
                RRecord rr = new RRecord
                (
                    xrec.Attribute("id")?.Value,
                    xrec.Attribute("type")?.Value,
                    xrec.Elements()
                        .Select<XElement, RProperty?>(p =>
                        {
                            string? pred = p.Attribute("prop")?.Value;
                            if (pred == null) return null;
                            if (p.Name == "field")
                            {
                                XAttribute? la = p.Attribute("{http://www.w3.org/XML/1998/namespace}lang");
                                return new RField
                                {
                                    Prop = pred,
                                    Value = p.Value,
                                    Lang = (la == null ? "" : la.Value)
                                };
                            }
                            else if (p.Name == "direct")
                            {
                                return new RLink { Prop = pred, Resource = p.Element("record").Attribute("id").Value };
                                //return new RDirect { Prop = pred, DRec = }
                            }
                            if (p.Name == "inverse")
                            {
                                return new RInverseLink { Prop = pred, Source = p.Element("record").Attribute("id").Value };
                            }
                            else return null;
                        })
                        .Where(ob => ob != null)
                        .Cast<RProperty>()
                        .ToArray(),
                    this
                );
                return rr;
            };
            return null;
        }
        IEnumerable<RRecord> SearchRRecords(string sample, bool bywords)
        {
            var xrecs = bywords ? SearchByWords(sample) : SearchByName(sample);
            foreach (var xrec in xrecs)
            {
                RRecord rr = new RRecord
                (
                    xrec.Attribute("id").Value,
                    xrec.Attribute("type").Value,
                    xrec.Elements()
                        .Select<XElement, RProperty?>(p =>
                        {
                            string? pred = p.Attribute("prop")?.Value;
                            if (pred == null) return null;
                            if (p.Name == "field")
                            {
                                return new RField { Prop = pred, Value = p.Value, Lang = "ru" };
                            }
                            else if (p.Name == "direct")
                            {
                                return new RLink { Prop = pred, Resource = p.Element("record").Attribute("id").Value };
                                //return new RDirect { Prop = pred, DRec = }
                            }
                            if (p.Name == "inverse")
                            {
                                //return new RInverseLink { Prop = pred, Source = p.Element("record").Attribute("id").Value };
                                // Базово, в поиске нет обратных отношений
                                return null;
                            }
                            else return null;
                        })
                        .Where(ob => ob != null)
                        .Cast<RProperty>()
                        .ToArray(),
                    this
                );
                yield return rr;
            }
        }
    }
}
