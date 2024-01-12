using Factograph.Data.Adapters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace Factograph.Data.Adapters
{
    /// <summary>
    /// Простая база данных на основе Xml-построения. Xml база данных объединяет записи fog-документов
    /// без дублирования и цепочек эквивалентности. А еще имеется словарь идентификатор-{запись, список обратных элементов}. 
    /// </summary>
    public class XmlDbAdapter : DAdapter
    {
        private class DbNode
        {
            public string id; // Нужен поскольку определяющая запись может быть отсутствовать
            public XElement xel;
            public List<XElement> inverse;
        }
        private XElement db = null;
        private Dictionary<string, DbNode> records = null;
        private Action<string> errors = s => { Console.WriteLine(s); };

        // Может быть null или будет преобразовывать слова в "нормализованные" слова
        private Func<string, string>? Normalize;
        public XmlDbAdapter(Func<string, string>? normalizeword)
        {
            this.Normalize = normalizeword;
        }

        /// <summary>
        /// Инициирование базы данных
        /// </summary>
        /// <param name="connectionstring">префикс варианта базы данных xml:, больше в connectionstring ничего не существенно </param>
        public override void Init(string connectionstring)
        {
            Console.WriteLine("Init of XmlDbAdapter " + DateTime.Now);
        }
        // Загрузка базы данных
        public override void StartFillDb(Action<string> turlog)
        {
            db = new XElement("db");
        }
        public override void FinishFillDb(Action<string> turlog)
        {
            records = new Dictionary<string, DbNode>();
            DbNode node;
            foreach (XElement xel in db.Elements())
            {
                // Это есть запись, которую надо зафиксировать под ее именем
                string id = xel.Attribute(ONames.rdfabout).Value;
                if (records.TryGetValue(id, out node))
                { // есть определение
                    node.xel = xel;
                }
                else
                {
                    records.Add(id, new DbNode() { id = id, xel = xel, inverse = new List<XElement>() });
                }
                // Теперь проанализируем прямые ссылки
                foreach (XElement el in xel.Elements())
                {
                    XAttribute res_att = el.Attribute(ONames.rdfresource);
                    if (res_att == null) continue;
                    string resource_id = res_att.Value;
                    if (records.TryGetValue(resource_id, out node))
                    { // есть определение
                        node.inverse.Add(el);
                    }
                    else
                    {
                        records.Add(resource_id,
                            new DbNode()
                            {
                                id = resource_id,
                                inverse = Enumerable.Repeat<XElement>(el, 1).ToList()
                            });
                    }
                }
            }
            // Заполнение wordsDic
            foreach (XElement xel in db.Elements())
            {
                string id = xel.Attribute(ONames.rdfabout)?.Value;
                var words = toWords(xel);
                foreach (string w in words)
                {
                    List<string> list;
                    if (wordsDic.TryGetValue(w, out list)) list.Add(id);
                    else
                    {
                        list = new List<string>(new string[] { id });
                        wordsDic.Add(w, list);
                    }

                }
            }
        }
        internal class XElementRdfSame : EqualityComparer<XElement>
        {
            public override bool Equals(XElement b1, XElement b2)
            {
                if (b1 == null && b2 == null)
                    return true;
                else if (b1 == null || b2 == null)
                    return false;

                return (b1.Attribute(ONames.rdfabout).Value ==
                        b2.Attribute(ONames.rdfabout).Value);
            }

            public override int GetHashCode(XElement bx)
            {
                string hCode = bx.Attribute(ONames.rdfabout).Value;
                return hCode.GetHashCode();
            }
        }
        internal class XRecordSame : EqualityComparer<XElement>
        {
            public override bool Equals(XElement b1, XElement b2)
            {
                if (b1 == null && b2 == null)
                    return true;
                else if (b1 == null || b2 == null)
                    return false;
                return (b1.Attribute("id").Value ==
                        b2.Attribute("id").Value);
            }
            public override int GetHashCode(XElement bx)
            {
                string hCode = bx.Attribute("id").Value;
                return hCode.GetHashCode();
            }
        }

        public override IEnumerable<XElement> SearchByName(string searchstring) //TODO: сделать с хешем
        {
            string ss = searchstring.ToLower();
            var rdfSame = new XElementRdfSame();
            //var query1 = db.Elements()
            //    .Where(xel => xel.Elements("{http://fogid.net/o/}name").Any(f => f.Value.ToLower().StartsWith(ss)))
            //    .ToArray();
            var qu = db.Elements()
                .SelectMany(xel => xel.Elements().Where(el =>
                    el.Name == "{http://fogid.net/o/}name" || el.Name == "{http://fogid.net/o/}alias"))
                .Where(el => el.Value.ToLower().StartsWith(ss))
                .ToArray();
            var query = db.Elements()
                .SelectMany(xel => xel.Elements().Where(el =>
                    el.Name == "{http://fogid.net/o/}name" || el.Name == "{http://fogid.net/o/}alias"))
                .Where(el => el.Value.ToLower().StartsWith(ss))
                .Select(el => el.Parent)
                .Select(el =>
                {
                    if (el.Name == "{http://fogid.net/o/}naming")
                    {
                        string referred = el.Element("{http://fogid.net/o/}referred-sys")?
                            .Attribute(ONames.rdfresource)?.Value;
                        if (referred != null && records.TryGetValue(referred, out DbNode node))
                        {
                            return node.xel;
                        }
                        else return el;
                    }
                    else return el;
                })
                .Distinct<XElement>(rdfSame)
                .Select(el => new XElement("record", new XAttribute("id", el.Attribute(ONames.rdfabout).Value),
                    new XAttribute("type", el.Name.NamespaceName + el.Name.LocalName),
                    el.Elements()
                        .Select(ff =>
                        {
                            XAttribute res_att = ff.Attribute(ONames.rdfresource);
                            string prop_name = ff.Name.NamespaceName + ff.Name.LocalName;
                            if (res_att == null)
                            {
                                return new XElement("field", new XAttribute("prop", prop_name), ff.Value);
                            }
                            else
                            {
                                return new XElement("direct", new XAttribute("prop", prop_name),
                                    new XElement("record", new XAttribute("id", res_att.Value)));
                            }
                        })
                    ))
                .ToArray()
                ;
            //.Select(el => new XElement("record", new XAttribute("id", el.Parent.Attribute(ONames.rdfabout).Value)));
            return query;
        }
        private static char[] delimeters = new char[] { ' ', '\n', '\t', ',', '.', ':', '-', '!', '?', '\"', '\'', '=', '\\', '|', '/',
                '(', ')', '[', ']', '{', '}', ';', '*', '<', '>'};
        private static string[] propnames = new string[]
        {
            "http://fogid.net/o/name",
            "http://fogid.net/o/alias",
            "http://fogid.net/o/description",
            "http://fogid.net/o/doc-content"
        };
        private IEnumerable<string> toWords(XElement xrec)
        {
            var query = xrec.Elements()
                .Where(xel => propnames.Contains(xel.Name.NamespaceName + xel.Name.LocalName))
                .SelectMany(xel =>
                {
                    string line = (string)xel.Value.ToLower();
                    var words = line.Split(delimeters, StringSplitOptions.RemoveEmptyEntries);
                    return words.Select(w =>
                    {
                        string wrd = w;
                        if (Normalize != null)
                        {
                            wrd = Normalize(w);
                        }
                        return wrd;
                    });
                });
            return query;
        }


        /// <summary>
        /// Словарь, сопоставляющий нормализованным словам списки идентификаторов 
        /// элементов из db, в которых эти слова присутствуют.
        /// </summary>
        private Dictionary<string, List<string>> wordsDic = new Dictionary<string, List<string>>();
        public override IEnumerable<XElement> SearchByWords(string searchwords)
        {
            var xSame = new XRecordSame();
            string[] wrds = searchwords.ToLower().Split(delimeters);

            var qq = wrds.SelectMany(w =>
            {
                string wrd = w;
                if (Normalize != null)
                {
                    wrd = Normalize(w);
                }
                List<string> xels;
                if (wordsDic.TryGetValue(wrd, out xels))
                {
                }
                else xels = new List<string>();
                return xels.Select(x => new { id = x, wrd = wrd });
            }).ToArray();
            var qqq = qq
                .GroupBy(iw => iw.id)
                .Select(gr => new { key = gr.Key, c = gr.Count(), o = gr.First() })
                .OrderByDescending(tri => tri.c)
                .Take(20).ToArray();
            var query = qqq.Select(tri =>
            {
                XElement res = GetItemByIdBasic(tri.o.id, false);
                if (res.Attribute("type").Value == "http://fogid.net/o/naming")
                {
                    XElement xref = res.Elements("direct")
                        .FirstOrDefault(d =>
                            d.Attribute("prop").Value == "http://fogid.net/o/referred-sys");
                    string idd = xref.Element("record").Attribute("id").Value;
                    if (idd != null) res = GetItemByIdBasic(idd, false);
                }
                return res;
            })
                .Distinct<XElement>(xSame)
                ;
            return query;
        }

        public override XElement GetItemByIdBasic(string id, bool addinverse)
        {
            DbNode node;
            if (records.TryGetValue(id, out node))
            {
                XElement xel = node.xel;
                if (xel == null) return null;
                string type = xel.Name.NamespaceName + xel.Name.LocalName;
                XElement xresult = new XElement("record", new XAttribute("id", id),
                    new XAttribute("type", type),
                    xel.Elements()
                    .Select<XElement, XElement>(el =>
                    {
                        XAttribute resource = el.Attribute(ONames.rdfresource);
                        string prop = el.Name.NamespaceName + el.Name.LocalName;
                        if (resource == null)
                        {
                            XAttribute xlang = el.Attribute("{http://www.w3.org/XML/1998/namespace}lang");
                            return new XElement("field", new XAttribute("prop", prop), xlang == null ? null : new XAttribute(xlang), el.Value);
                        }
                        else
                        {
                            //if (el.Name == ONames.rdftype) return null;
                            return new XElement("direct", new XAttribute("prop", prop),
                                new XElement("record", new XAttribute("id", resource.Value)));
                        }
                    }),
                    addinverse ?
                    node.inverse
                    .Select(inv => new XElement("inverse", new XAttribute("prop", inv.Name.NamespaceName + inv.Name.LocalName),
                        new XElement("record", new XAttribute("id", inv.Parent.Attribute(ONames.rdfabout).Value)))) :
                    null);
                return xresult;
            }
            return null;
        }
        public override XElement GetItemById(string id, XElement format)
        {
            DbNode node;
            if (!records.TryGetValue(id, out node)) return null;
            return GetItemByNode(node, format);
        }
        private XElement GetItemByNode(DbNode node, XElement format)
        {
            XElement xel = node.xel;
            if (xel == null) return null;
            string type = xel.Name.NamespaceName + xel.Name.LocalName;
            return new XElement("record",
                new XAttribute("id", xel.Attribute(ONames.rdfabout).Value),
                new XAttribute("type", type),
                format.Elements().Where(fel => fel.Name == "field" || fel.Name == "direct" || fel.Name == "inverse")
                .SelectMany(fel =>
                {
                    string prop = fel.Attribute("prop").Value;
                    if (fel.Name == "field")
                    {
                        return xel.Elements()
                            .Where(el => el.Name.NamespaceName + el.Name.LocalName == prop)
                            .Select(el => new XElement("field", new XAttribute("prop", prop),
                                el.Attribute("{http://www.w3.org/XML/1998/namespace}lang") == null ? null :
                                    new XAttribute(el.Attribute("{http://www.w3.org/XML/1998/namespace}lang")),
                                el.Value));
                    }
                    else if (fel.Name == "direct")
                    {
                        return xel.Elements()
                            .Where(el => el.Name.NamespaceName + el.Name.LocalName == prop)
                            .Select<XElement, XElement>(el =>
                            {
                                DbNode node2;
                                if (!records.TryGetValue(el.Attribute(ONames.rdfresource).Value, out node2) || node2.xel == null) return null;
                                string t = node2.xel.Name.NamespaceName + node2.xel.Name.LocalName;
                                XElement f = fel.Elements("record")
                                    .FirstOrDefault(fr =>
                                    {
                                        XAttribute t_att = fr.Attribute("type");
                                        if (t_att == null) return true;
                                        return t_att.Value == t;
                                    });
                                if (f == null) return null;
                                return new XElement("direct", new XAttribute("prop", prop),
                                    GetItemByNode(node2, f));
                            });
                    }
                    else if (fel.Name == "inverse")
                    {
                        //return node.inverse
                        //    .Where(el => el.Name.NamespaceName + el.Name.LocalName == prop)
                        //    .Select(el => new XElement("inverse", new XAttribute("prop", prop),
                        //        fel.Element("record") == null ? null :
                        //        GetItemById(el.Parent.Attribute(ONames.rdfabout).Value, fel.Element("record"))));
                        return node.inverse
                            .Where(el => el.Name.NamespaceName + el.Name.LocalName == prop)
                            .Select<XElement, XElement>(el =>
                            {
                                DbNode node2;
                                if (!records.TryGetValue(el.Parent.Attribute(ONames.rdfabout).Value, out node2) || node2.xel == null) return null;
                                string t = node2.xel.Name.NamespaceName + node2.xel.Name.LocalName;
                                XElement f = fel.Elements("record")
                                    .FirstOrDefault(fr =>
                                    {
                                        XAttribute t_att = fr.Attribute("type");
                                        if (t_att == null) return true;
                                        return t_att.Value == t;
                                    });
                                if (f == null) return null;
                                return new XElement("inverse", new XAttribute("prop", prop),
                                    GetItemByNode(node2, f));
                            });
                    }
                    else return null;
                }));
        }

        //public override XElement GetItemByIdSpecial(string id)
        //{
        //    return GetItemByIdBasic(id, true);
        //}


        private XElement RemoveRecord(string id)
        {
            DbNode node;
            if (records.TryGetValue(id, out node))
            {
                // Удаляемый объект состоит из записи и из элементов-ссылок, попавших в "чужие" списки
                // Сначала "работаем" по чужим спискам
                XElement xel = node.xel;
                foreach (XElement el in xel.Elements())
                {
                    XAttribute resource = el.Attribute(ONames.rdfresource);
                    if (resource == null) continue;
                    // находим список
                    string id2 = resource.Value;
                    DbNode node2;
                    if (records.TryGetValue(id2, out node2))
                    {
                        // Уберем из списка
                        node2.inverse.Remove(el);
                    }
                }
                // Открепляем из базы данных
                xel.Remove();
                return xel;
            }
            return null;
        }
        public override XElement Delete(string id)
        {
            XElement record = RemoveRecord(id);
            if (record == null) return null;
            records.Remove(id);
            return record;
        }

        private XElement Add(XElement record)
        {
            // Работаем по "чужим спискам
            XElement xel = new XElement(record);
            foreach (XElement el in xel.Elements())
            {
                XAttribute resource = el.Attribute(ONames.rdfresource);
                if (resource == null) continue;
                // находим список
                string id2 = resource.Value;
                DbNode node2;
                if (records.TryGetValue(id2, out node2))
                {
                    // добавляем в список
                    node2.inverse.Add(el);
                }
            }

            // Прикрепляем (зачем-то) запись к базе данных
            db.Add(xel);
            // Добавляем к словарю
            string id = record.Attribute(ONames.rdfabout).Value;

            DbNode node;
            if (records.TryGetValue(id, out node))
            { // Если узел есть, то прикрепляем к полю узла
                node.xel = xel;
            }
            else
            { // Если узла нет, то создаем
                records.Add(id, new DbNode() { id = id, xel = xel, inverse = new List<XElement>() });
            }
            return xel;
        }

        private XElement AddUpdate(XElement record)
        {
            string id = record.Attribute(ONames.rdfabout).Value;
            DbNode node;
            if (records.TryGetValue(id, out node))
            {
                // Будем перебирать элементы "старого" значения и, если таких нет, добавлять в новое
                foreach (XElement old_el in node.xel.Elements())
                {
                    XAttribute l_att = old_el.Attribute("{http://www.w3.org/XML/1998/namespace}lang");
                    if (record.Elements().Any(el => el.Name == old_el.Name &&
                        (l_att == null ? true :
                            (el.Attribute("{http://www.w3.org/XML/1998/namespace}lang") != null &&
                                l_att.Value == el.Attribute("{http://www.w3.org/XML/1998/namespace}lang").Value)))) continue;
                    // старые ссылки не копируем
                    if (old_el.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource") != null) continue;
                    record.Add(old_el);
                }
                RemoveRecord(id);
            }
            Add(record);
            return record;
        }

        public override void Close()
        {
            db = null;
            records = null;
        }

        public override IEnumerable<XElement> GetAll()
        {
            return db.Elements();
        }

        public override XElement PutItem(XElement record)
        {
            if (record.Name.LocalName == "delete")
            {
                Delete(record.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value);
                return null;
            }
            else if (record.Name.LocalName == "substitute")
            {
                return null;
            }
            else
            {
                return AddUpdate(record);
            }
        }

        public override void LoadXFlow(IEnumerable<XElement> xflow, Dictionary<string, string> orig_ids)
        {
            foreach (XElement record in xflow)
            {
                string id = record.Attribute(ONames.rdfabout).Value;
                //if (id == "Mc2816_1142") { }
                // Корректируем идентификатор
                if (orig_ids.TryGetValue(id, out string idd)) id = idd;
                if (id == null) continue;
                //int rec_type = store.CodeEntity(ONames.fog + record.Name.LocalName);
                //int id_ent = store.CodeEntity(id);
                //XElement xrecord = new XElement(record.Name,
                //    record.Elements().Where(el => el.Attribute(ONames.rdfresource) != null)
                //            .Select(subel =>
                //            {
                //                int prop = store.CodeEntity(subel.Name.NamespaceName + subel.Name.LocalName);
                //                string resource = subel.Attribute(ONames.rdfresource).Value;
                //                if (orig_ids.TryGetValue(resource, out string res)) if (res != null) resource = res;
                //                return new object[] { prop, store.CodeEntity(resource) };
                //            })).ToArray()
                //        ,
                //        record.Elements().Where(el => el.Attribute(ONames.rdfresource) == null)
                //            .Select(subel =>
                //            {
                //                int prop = store.CodeEntity(subel.Name.NamespaceName + subel.Name.LocalName);
                //                return new object[] { prop, subel.Value };
                //            })
                //            .ToArray()
                //        };
                XElement nrec = new XElement(record.Name, new XAttribute(ONames.rdfabout, id),
                    record.Elements().Where(el => el.Attribute(ONames.rdfresource) == null)
                        .Select(subel =>
                        {
                            XName prop = subel.Name;
                            XAttribute lg = subel.Attribute("{http://www.w3.org/XML/1998/namespace}lang");
                            XElement res = new XElement(prop, subel.Value, lg == null ? null : new XAttribute(lg));
                            return res;
                        }),
                    record.Elements().Where(el => el.Attribute(ONames.rdfresource) != null)
                        .Select(subel =>
                        {
                            XName prop = subel.Name;
                            string resource = subel.Attribute(ONames.rdfresource).Value;
                            if (orig_ids.TryGetValue(resource, out string res)) if (res != null) resource = res;
                            return new XElement(prop, new XAttribute(ONames.rdfresource, resource));
                        }),
                    null);
                db.Add(nrec);

            }

        }

        public override object GetRecord(string id, bool addinverse)
        {
            throw new NotImplementedException();
        }
    }
}
