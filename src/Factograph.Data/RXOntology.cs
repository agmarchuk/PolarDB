using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using System.Threading.Tasks;
using Factograph.Data;
//using RDFEngine;

namespace Factograph.Data
{
    public class RXOntology : IOntology
    {
        public RXOntology(string path)
        {
            // Действие для "Классной кухни"
            xontology = XElement.Load(path);
            this.BuldRTree();

            // ============== Вычисление таблицы перечислимых DatatypeProperty id -> XElement EnumerationType

            // Сначала построим вспомогательную таблицу спецификаций перечислимых типов
            Dictionary<string, XElement> enumerationTypes = xontology.Elements("EnumerationType")
                .ToDictionary(x => x.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value);

            // Теперь берем определениz я всех DatatypeProperty, оставляем те, range которых входит в предыдущую таблицу 
            // и строим то, что нужно
            enufildspecs = xontology.Elements("DatatypeProperty")
                .Where(dp =>
                {
                    string resource = dp.Element("range")?.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")?.Value;
                    if (resource == null) return false;
                    if (!enumerationTypes.ContainsKey(resource)) return false;
                    return true;
                })
                .ToDictionary(
                    dp => dp.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value,
                    dp => enumerationTypes[dp.Element("range")?.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")?.Value]);


            // ============== конец

            rontology = LoadROntology(path);
            // Это словарь онтологических описаний: по идентификатору онто объекта дается номер в таблице описаний
            dicOnto = rontology
               .Select((rr, nom) => new { V = rr.Id, nom })
               .ToDictionary(pair => pair.V, pair => pair.nom);
            priorityDictionary =
                rontology.Select(rr => new { V = rr.Id, pr = rr.GetField("priority") })
               .ToDictionary(pair => pair.V, pair => pair.pr);


            dicsProps = new Dictionary<string, int>[rontology.Length];
            for (int i = 0; i < rontology.Length; i++)
            {
                if (rontology[i].Props != null)
                {
                    RLink[] links = rontology[i].Props
                        .Where(p => (p.Prop == "DatatypeProperty" || p.Prop == "ObjectProperty"))
                        .Cast<RLink>().ToArray();
                    dicsProps[i] = links
                        .Select((p, n) => new { V = p.Resource, n })
                        .ToDictionary(pair => pair.V, pair => pair.n);
                }
            }
            // Вычисляем обратные свойства для типов
            //dicsInversePropsForType = rontology.Where(rr => rr.Tp == "ObjectProperty")
            //    .SelectMany(rr => rr.Props
            //        .Where(p => p is RLink && p.Prop == "range")
            //        .Select(p => new { pr = rr.Id, tp = ((RLink)p).Resource }))
            //    .GroupBy(pair => pair.tp)
            //    .ToDictionary(keypair => keypair.Key, keypair => keypair.Select(x => x.pr).ToArray());
            dicsInversePropsForType = //null;
                rontology.Where(rr => rr.Tp == "ObjectProperty")
                .SelectMany(rr => rr.Props
                    .Where(p => p is RLink && p.Prop == "range")
                    .Select(p => new { pr = rr.Id, tp = ((RLink)p).Resource }))
                .SelectMany(pa => DescendantsAndSelf(pa.tp).Select(t => new { ty = t, pr_id = pa.pr }))
                .GroupBy(typr => typr.ty)
                .ToDictionary(keypair => keypair.Key, keypair => keypair.Select(x => x.pr_id).Distinct().ToArray());

            // Для каждого типа создадим по 2 словаря, а потом объединим их под общим словарем
            dicsDirectPropsForType = //null;
                rontology.Where(rr => rr.Tp == "ObjectProperty" || rr.Tp == "DatatypeProperty")
                .SelectMany(rr => rr.Props
                    .Where(p => p is RLink && p.Prop == "domain")
                    .Select(p => new { pr = rr.Id, tp = ((RLink)p).Resource }))
                .SelectMany(pa => DescendantsAndSelf(pa.tp).Select(t => new { ty = t, pr_id = pa.pr }))
                .GroupBy(typr => typr.ty)
                .ToDictionary(keypair => keypair.Key, keypair => keypair.Select(x => x.pr_id).Distinct().ToArray());
            // ОПределение функции. 


        }

        public IEnumerable<string> AncestorsAndSelf(string id)
        {
            RTreeNode node = RTNdic[id];
            var res = AAS(node).Select(n => n.Id);
            return res;
        }
        public IEnumerable<string> DescendantsAndSelf(string id)
        {
            RTreeNode node = RTNdic[id];
            return DAS(node).Select(n => n.Id);
        }

        // ============================= "Классная кухня" - Конечное построение - предки и потомки ==============
        private XElement xontology; // Сюда загрузчик поместит онтологию 
        // словарь узлов классов
        private Dictionary<string, RTreeNode> RTNdic;
        // Создание словаря из XML-онтологии. Загружается когда есть xontology
        private void BuldRTree()
        {
            string rdf = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}";
            // Создадим узлы, поместим их в дерево
            RTNdic = xontology.Elements("Class").Select(x => new RTreeNode
            {
                Id = x.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value,
                Childs = new List<RTreeNode>()
            }).ToDictionary(t => t.Id);
            // Снова сканируем элементы, заполняем родителя и детей
            foreach (XElement x in xontology.Elements("Class"))
            {
                string parentId = x.Element("SubClassOf")?.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")?.Value;
                if (parentId == null) continue;
                RTreeNode node = RTNdic[x.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value];
                RTreeNode parentNode = RTNdic[parentId];
                node.Parent = parentNode;
                parentNode.Childs.Add(node);
            }
        }

        private IEnumerable<RTreeNode> AAS(RTreeNode node)
        {
            if (node.Parent == null) return new RTreeNode[] { node };
            var res = AAS(node.Parent).Append(node);
            return res;
        }
        // 
        private IEnumerable<RTreeNode> DAS(RTreeNode node)
        {
            return (new RTreeNode[] { node }).Concat(node.Childs.SelectMany(c => DAS(c)));
        }
        // ======================== конец "кухни" =========================


        // Массив определений
        private RRecord[] rontology = null;
        // Словарь онтологических объектов имя -> номер в массивах
        private Dictionary<string, int> dicOnto = null;
        private Dictionary<string, string> priorityDictionary = null;

        /// <summary>
        /// Массив словарей свойств для записей. Элементы массива позиционно соответствуют массиву утверждений.
        /// Элемент массива - словарь, отображений имен свойств в номера позиции в массиве онтологии.
        /// </summary>
        private Dictionary<string, int>[] dicsProps = null;
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, string[]> dicsInversePropsForType = null;
        private Dictionary<string, string[]> dicsDirectPropsForType = null;

        public IEnumerable<string> GetInversePropsByType(string tp)
        {
            return dicsInversePropsForType[tp].OrderBy(name => priorityDictionary[name]);
        }
        public IEnumerable<string> GetDirectPropsByType(string tp)
        {
            return dicsDirectPropsForType[tp].OrderBy(name => priorityDictionary[name]);
        }
        public int PropsTotal(string tp)
        {
            int n1 = dicsDirectPropsForType[tp].Length;
            int n2 = dicsInversePropsForType[tp].Length;
            return n1 + n2;
        }
        public int PropPosition(string tp, string prop, bool isinverse)
        {
            var d1 = dicsDirectPropsForType[tp];
            int n1 = d1.Length;
            if (isinverse) d1 = dicsInversePropsForType[tp];
            int i = 0;
            for (; i < d1.Length; i++)
            {
                if (d1[i] == prop) break;
            }
            if (i == d1.Length) return -1;
            if (isinverse) i += n1;
            return i;
        }

        // Словарь родителей с именами родителей.
        private Dictionary<string, string[]> parentsDictionary = null;
        public Dictionary<string, string[]> ParentsDictionary { get { return parentsDictionary; } }

        RRecord[] IOntology.OntoSpec { get { return rontology; } }

        private RRecord[] LoadROntology(string path)
        {
            string rdf = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}";
            Func<XElement, string> ename = x => x.Name.NamespaceName + x.Name.LocalName;

            List<RRecord> resultList = new List<RRecord>();
            parentsDictionary = new Dictionary<string, string[]>();

            foreach (var el in xontology.Elements())
            {
                // Входными элементами являются: Class, DatatypeProperty, ObjectProperty, EnumerationType


                string recId = el.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value;

                var subcl = el.Element("SubClassOf")?.Attribute(rdf + "resource")?.Value;
                var myClasses = getSubClasses(el, xontology);
                parentsDictionary.Add(recId, myClasses);

                List<RProperty> propsList = new List<RProperty>();
                // el.Elements("label").Select(l => new RField() { Prop = "Label", Value = l.Value })
                var lls = el.Elements("label").ToArray();
                foreach (var label in el.Elements("label"))
                {
                    if (label?.Value != null) propsList.Add(
                        new RField()
                        {
                            Prop = "Label",
                            Value = label.Value,
                            Lang = label.Attribute("{http://www.w3.org/XML/1998/namespace}lang")?.Value
                        });
                }
                foreach (var invlabel in el.Elements("inverse-label"))
                {
                    if (invlabel?.Value != null) propsList.Add(
                        new RField()
                        {
                            Prop = "InvLabel",
                            Value = invlabel.Value,
                            Lang = invlabel.Attribute("{http://www.w3.org/XML/1998/namespace}lang")?.Value
                        });
                }
                propsList.Add(new RField() { Prop = "priority", Value = el.Attribute("priority")?.Value });

                var sortedProps = xontology.Elements()
                    .Where(x => (x.Name == "ObjectProperty" || x.Name == "DatatypeProperty")
                        && myClasses.Contains(x.Element("domain").Attribute(rdf + "resource").Value))
                    .OrderBy(prop => prop.Attribute("priority")?.Value);

                propsList.AddRange(sortedProps.Select(p => new RLink { Prop = ename(p), Resource = p.Attribute(rdf + "about").Value }));

                propsList.AddRange(el.Elements("domain").Select(x => new RLink { Prop = "domain", Resource = x.Attribute(rdf + "resource").Value }));
                propsList.AddRange(el.Elements("range").Select(x => new RLink { Prop = "range", Resource = x.Attribute(rdf + "resource").Value }));


                // Во всех случаях, в выходной поток направляется RRecord, причем тип записи совпадает с именем элемента,
                // идентификатор - берется из rdf:about
                RRecord rec = new RRecord(
                    recId,
                    ename(el),
                    propsList.ToArray(),
                    null);
                resultList.Add(rec);

            }
            var arr = resultList.ToArray();
            return arr;
        }

        private string[] getSubClasses(XElement el, XElement ontology, string[] tempArr)
        {
            var recId = el.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value;
            string rdf = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}";
            tempArr = tempArr.Append(recId).ToArray();
            if (el.Element("SubClassOf") == null)
            {
                return tempArr;
            }
            else
            {
                return getSubClasses(
                    ontology.Elements().FirstOrDefault(x =>
                    x.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about").Value == el.Element("SubClassOf").Attribute(rdf + "resource").Value),
                    ontology, tempArr);
            }
        }
        private string[] getSubClasses(XElement el, XElement ontology)
        {
            return getSubClasses(el, ontology, new string[] { });
        }

        // Таблица перечислимых DatatypeProperty id -> XElement EnumerationType
        private Dictionary<string, XElement> enufildspecs;
        public bool IsEnumeration(string prop) => enufildspecs.ContainsKey(prop);
        public string? EnumValue(string prop, string val, string lang)
        {
            if (val == null || !enufildspecs.ContainsKey(prop)) return null;
            XElement spec = enufildspecs[prop];

            //var state = spec.Elements("state")
            //    .Where(s => s.Attribute("value").Value == val)
            //    .Aggregate((acc, s) =>
            //    {
            //        if (acc == null) return s;
            //        string lan = acc.Attribute("{http://www.w3.org/XML/1998/namespace}lang")?.Value;
            //        if (lan == null) return s;
            //        if (lan == lang) return acc;
            //        string lan1 = s.Attribute("{http://www.w3.org/XML/1998/namespace}lang")?.Value;
            //        if (lan1 == null) return acc;
            //        if (lan1 == lang || lan1 == "en") return s;
            //        return acc;
            //    });
            var state = spec.Elements("state")
                .Where(s => s.Attribute("value")?.Value == val)
                .FirstOrDefault(s => s.Attribute("{http://www.w3.org/XML/1998/namespace}lang")?.Value == "ru");
            if (state == null) return null;
            return state.Value;
        }
        public KeyValuePair<string, string>[] EnumPairs(string prop, string lang)
        {
            if (!enufildspecs.ContainsKey(prop)) return null;
            XElement spec = enufildspecs[prop];
            var states = spec.Elements("state")
                .Where(s => s.Attribute("{http://www.w3.org/XML/1998/namespace}lang").Value == lang)
                .Select(s => KeyValuePair.Create(s.Attribute("value").Value, s.Value))
                .ToArray();

            return states;
        }

        /// <summary>
        /// Формирует из записи набор "столбцов" в виде вариантов RProperty, опираясь на данную онтологию.
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        //public RProperty[] ReorderFieldsDirects(RRecord record, string lang)
        //{
        //    // Определяем тип, по нему номер спецификации, по нему спецификацию из rontology. Назовем ее columns
        //    string tp = record.Tp;
        //    int nom = dicOnto[tp];
        //    var columns = rontology[nom];
        //    Dictionary<string, int> dicProps = dicsProps[nom];

        //    // Определяем количество полей, строим результирующий массив
        //    RProperty[] res_arr = new RProperty[dicProps.Count()];

        //    // Проходимся по колонкам, заполняем элементы res_arr пустыми значениями 
        //    // TODO: можно эти массивы вычислить заранее, но стоит ли? Все равно для работы потебутеся копия
        //    foreach (var col in columns.Props)
        //    {
        //        if (col is RLink)
        //        {
        //            RLink rl = (RLink)col;
        //            int n = dicProps[rl.Resource];
        //            if (rl.Prop == "DatatypeProperty") res_arr[n] = new RField { Prop = rl.Resource };
        //            else if (rl.Prop == "ObjectProperty") res_arr[n] = new RDirect { Prop = rl.Resource };
        //            else throw new Exception("Err: 931891");
        //        }
        //    }

        //    // Пройдемся по свойствам обрабатываемой записи rrecord, значения скопируем в выходной массив на соответствующей позиции
        //    foreach (var p in record.Props)
        //    {
        //        if (p == null) continue;
        //        if (dicProps.ContainsKey(p.Prop))
        //        {
        //            int n = dicProps[p.Prop];
        //            if (p is RField)
        //            {
        //                RField f = (RField)p;
        //                // Если имеющееся значение пустое, то переписать из f Value и Lang
        //                if (((RField)res_arr[n]).Value == null)
        //                {
        //                    ((RField)res_arr[n]).Value = f.Value;
        //                    ((RField)res_arr[n]).Lang = f.Lang;
        //                }
        //                else // Иначе есть два варианта: всепобеждающий lang и английский
        //                {
        //                    if ((((RField)res_arr[n]).Lang ?? "ru") == lang) { }
        //                    else if ((f.Lang ?? "ru") == lang)
        //                    {
        //                        ((RField)res_arr[n]).Value = f.Value;
        //                        ((RField)res_arr[n]).Lang = f.Lang;
        //                    }
        //                    else if (f.Lang == "en")
        //                    {
        //                        ((RField)res_arr[n]).Value = f.Value;
        //                        ((RField)res_arr[n]).Lang = f.Lang;
        //                    }
        //                }
        //            }
        //            else if (p is RDirect)
        //            {
        //                RDirect d = (RDirect)p;
        //                ((RDirect)res_arr[n]).DRec = d.DRec;
        //            }
        //        }
        //        else
        //        {

        //        }

        //    }

        //    return res_arr;
        //}

        public IEnumerable<string> RangesOfProp(string prop)
        {
            int nom = dicOnto[prop];
            return rontology[nom].Props
                .Where(p => p is RLink)
                .Cast<RLink>()
                .Where(rl => rl.Prop == "range")
                .Select(rl => rl.Resource);
        }
        public IEnumerable<string> DomainsOfProp(string prop)
        {
            int nom = dicOnto[prop];
            return rontology[nom].Props
                .Where(p => p is RLink)
                .Cast<RLink>()
                .Where(rl => rl.Prop == "domain")
                .Select(rl => rl.Resource);
        }
        public string LabelOfOnto(string id)
        {
            if (string.IsNullOrEmpty(id) || !dicOnto.ContainsKey(id)) return null;
            int nom = dicOnto[id];
            return rontology[nom].Props
                .Where(p => p is RField)
                .Cast<RField>()
                .FirstOrDefault(rl => rl.Prop == "Label")?.Value;
        }
        public string LabelOfOnto(string id, string lang)
        {
            if (string.IsNullOrEmpty(id) || !dicOnto.ContainsKey(id)) return null;
            int nom = dicOnto[id];
            return rontology[nom].Props
                .Where(p => p is RField)
                .Cast<RField>()
                .FirstOrDefault(rl => rl.Prop == "Label" && rl.Lang == lang)?.Value;
        }
        public string InvLabelOfOnto(string id)
        {
            int nom = dicOnto[id];
            return rontology[nom].Props
                .Where(p => p is RField)
                .Cast<RField>()
                .FirstOrDefault(rl => rl.Prop == "InvLabel")?.Value;
        }

        public IEnumerable<string> GetAllClasses()
        {
            var res = rontology
                .Where(definition => definition.Tp == "Class")
                .Select(definition => definition.Id)
                .ToArray();
            return res;
        }
    }

    // Класс для построения дерева онтологических классов
    class RTreeNode
    {
        public string Id { get; set; }
        public RTreeNode Parent { get; set; }
        public List<RTreeNode> Childs { get; set; }
    }
}
