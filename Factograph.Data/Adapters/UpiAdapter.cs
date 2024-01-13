using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Factograph.Data;
using Polar.DB;
using Factograph.Data.Adapters;
using Polar.Universal;

namespace Factograph.Data.Adapters
{
    public class UpiAdapter : DAdapter
    {
        // Адаптер состоит из последовательности записей, дополнительного индекса и последовательности триплетов объектных свойств 
        public PType tp_prop;
        public PType tp_rec;
        private PType tp_triple;
        private USequence records;
        private SVectorIndex names;
        private SVectorIndex svwords;

        private RRecordSame rSame;

        // Может быть null или будет преобразовывать слова в "нормализованные" слова
        private Func<string, string>? Normalize;
        private Func<object, bool> Isnull;

        // Констуктор инициализирует то, что можно и не изменяется
        public UpiAdapter(Func<string, string>? normalizeword)
        {
            this.Normalize = normalizeword;
            Console.WriteLine("mag: UniAdapter constructed " + DateTime.Now);
            tp_prop = new PTypeUnion(
                new NamedType("novariant", new PType(PTypeEnumeration.none)),
                new NamedType("field", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("value", new PType(PTypeEnumeration.sstring)),
                    new NamedType("lang", new PType(PTypeEnumeration.sstring)))),
                new NamedType("objprop", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("link", new PType(PTypeEnumeration.sstring)))),
                new NamedType("invlink", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("sourcelink", new PType(PTypeEnumeration.sstring))))
                );
            tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.sstring)),
                new NamedType("tp", new PType(PTypeEnumeration.sstring)),
                new NamedType("props", new PTypeSequence(tp_prop)));
            tp_triple = new PTypeRecord(
                new NamedType("subj", new PType(PTypeEnumeration.sstring)),
                new NamedType("pred", new PType(PTypeEnumeration.sstring)),
                new NamedType("obj", new PType(PTypeEnumeration.sstring)));
            Isnull = ob => (string)((object[])ob)[1] == "delete"; // в поле tp находится "delete" //TODO: проверить, что delete без пространств имен 

            // Компаратор
            rSame = new RRecordSame();
        }

        // Главный инициализатор. Используем connectionstring 
        private string dbfolder;
        private int file_no = 0;
        private char[] delimeters;
        public override void Init(string connectionstring)
        {
            Console.WriteLine("mag: Init of UpiAdapter: " + DateTime.Now);
            if (connectionstring != null && connectionstring.StartsWith("upi:"))
            {
                dbfolder = connectionstring.Substring("upi:".Length);
            }
            else dbfolder = connectionstring.Substring(4);

            this.nodatabase = false;
            if (!File.Exists(dbfolder + "0.bin"))
            {
                Console.Write("Reload db files");
                this.nodatabase = true;
            }
            Func<Stream> GenStream = () =>
            {
                try
                {
                    var ff = new FileStream(dbfolder + (file_no++) + ".bin", FileMode.OpenOrCreate, FileAccess.ReadWrite);
                    return ff;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message + " time=" + DateTime.Now);
                }
                return null;
            };


            // Полключение к базе данных
            Func<object, int> hashId = obj => Hashfunctions.HashRot13(((string)(((object[])obj)[0])));
            Comparer<object> comp_direct = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])a)[0];
                string val2 = (string)((object[])b)[0];
                return string.Compare(val1, val2, StringComparison.OrdinalIgnoreCase);
            }));

            // Создаем последовательность R-записей
            records = new USequence(tp_rec, dbfolder + "state.bin", GenStream,
                rec => (string)((object[])rec)[1] == "delete",
                rec => (string)((object[])rec)[0],
                str => Hashfunctions.HashRot13((string)str),
                true);

            Func<object, IEnumerable<string>> skey = obj =>
            {
                object[] props = (object[])((object[])obj)[2];
                var query = props.Where(p => (int)((object[])p)[0] == 1)
                    .Select(p => ((object[])p)[1])
                    .Cast<object[]>()
                    .Where(f => (string)f[0] == "http://fogid.net/o/name" ||
                        (string)f[0] == "http://fogid.net/o/alias")
                    .Select(f => (string)f[1]).ToArray();
                return query;
            };
            records.Refresh();

            GC.Collect();
            Console.WriteLine("======After Records Init === Total Memory: " + GC.GetTotalMemory(true));

            names = new SVectorIndex(GenStream, records, skey);
            names.Refresh();

            GC.Collect();
            Console.WriteLine("======After names Init === Total Memory: " + GC.GetTotalMemory(true));

            delimeters = new char[] { ' ', '\n', '\t', ',', '.', ':', '-', '!', '?', '\"', '\'', '=', '\\', '|', '/',
                '(', ')', '[', ']', '{', '}', ';', '*', '<', '>'};
            //string[] propnames = new string[] { "http://fogid.net/o/name", "http://fogid.net/o/description" };
            string[] propnames = new string[]
            {
                "http://fogid.net/o/name",
                "http://fogid.net/o/alias",
                "http://fogid.net/o/description",
                "http://fogid.net/o/doc-content"
            };
            Func<object, IEnumerable<string>> toWords = obj =>
            {
                object[] props = (object[])((object[])obj)[2];
                var query = props
                    .Where(p => (int)((object[])p)[0] == 1)
                    .Select(p => ((object[])p)[1])
                    .Cast<object[]>()
                    .Where(f => (propnames.Contains((string)f[0])))
                    .SelectMany(f =>
                    {
                        string line = (string)f[1];
                        var words = line.ToLower()
                            .Split(delimeters, StringSplitOptions.RemoveEmptyEntries);
                        return words.Select(w =>
                        {
                            string wrd = w;
                            if (Normalize != null)
                            {
                                wrd = Normalize(w);
                            }
                            return wrd;
                        });
                    }).ToArray();
                return query;
            };
            svwords = new SVectorIndex(GenStream, records, toWords);
            svwords.Refresh();

            GC.Collect();
            Console.WriteLine("======After svwords Init === Total Memory: " + GC.GetTotalMemory(true));

            records.uindexes = new IUIndex[]
            {
                names,
                svwords
            };

            GC.Collect();
            Console.WriteLine("======After Init === Total Memory: " + GC.GetTotalMemory(true));

        }
        public override void Close()
        {
            Console.WriteLine($"mag: UniAdapter Closing {DateTime.Now} {records == null} {names == null} {svwords == null}");
            records.Close();
            Console.WriteLine($"mag: UniAdapter Closed {DateTime.Now}");

        }


        public override void StartFillDb(Action<string> turlog)
        {
            records.Clear();
            // Индексы чистятся внутри
        }
        public override void FinishFillDb(Action<string> turlog)
        {
            records.Build();
            GC.Collect();
            // Состояние сохраняется в методе Build
        }
        public void RestoreDynamic()
        {
            // Восстановить динаические значения в индексах можно так:
            records.RestoreDynamic();
        }

        // Загрузка потока x-элементов
        internal class OTriple { public string subj, pred, resource; }
        internal class InverseLink { public string pred, source; }
        public override void LoadXFlow(IEnumerable<XElement> xflow, Dictionary<string, string> orig_ids)
        {
            // Соберем граф через 2 сканирования
            // Проход 1: вычисление обратных объектных свойств
            Dictionary<string, InverseLink[]> inverseProps = xflow.SelectMany(record =>
            {
                string id = record.Attribute(ONames.rdfabout).Value;
                //if (id == "Mc2816_1142") { }
                // Корректируем идентификатор -- ЭТО НЕ НУЖНО поскольку все переопределенные 
                // отсутствуют в потоке, но поставлю выбрасывание исключения
                if (orig_ids.TryGetValue(id, out string idd)) throw new Exception("e8eh2"); // id = idd;
                if (id == null) return new OTriple[0];
                var directProps = record.Elements().Where(el => el.Attribute(ONames.rdfresource) != null)
                    .Select(el =>
                    {
                        string subj = id;
                        string pred = el.Name.NamespaceName + el.Name.LocalName;
                        string resource = el.Attribute(ONames.rdfresource).Value;
                        if (orig_ids.TryGetValue(resource, out string res)) if (res != null) resource = res;
                        return new OTriple { subj = subj, pred = pred, resource = resource };
                    });
                return directProps;
            }).GroupBy(triple => triple.resource, triple => new InverseLink { pred = triple.pred, source = triple.subj })
            .Select(g => new { key = g.Key, ilinks = g.Select(lnk => lnk) })
            .ToDictionary(pair => pair.key, pair => pair.ilinks.ToArray());
            //Dictionary<string, InverseLink[]> inverseProps = new Dictionary<string, InverseLink[]>();

            //var test_rec = inverseProps["syp2001-p-marchuk_a"];

            // Проход 2: вычисление графа 
            IEnumerable<object> flow = xflow.Select(record =>
            {
                string id = record.Attribute(ONames.rdfabout).Value;
                // Корректируем идентификатор
                if (orig_ids.TryGetValue(id, out string idd)) id = idd;
                if (id == null) return null; // уничтоженный
                string rec_type = ONames.fog + record.Name.LocalName;
                object[] orecord = new object[] {
                    id,
                    rec_type,
                    record.Elements()
                        .Select(el =>
                        {
                            string prop = el.Name.NamespaceName + el.Name.LocalName;
                            if (el.Attribute(ONames.rdfresource) != null)
                            {
                                string resource = el.Attribute(ONames.rdfresource).Value;
                                if (orig_ids.TryGetValue(resource, out string res)) if (res != null) resource = res;
                                return new object[] { 2, new object[] { prop, resource } };
                            }
                            else
                            {
                                var xlang = el.Attribute("{http://www.w3.org/XML/1998/namespace}lang");
                                string lang = xlang == null ? "" : xlang.Value;
                                return new object[] { 1, new object[] { prop, el.Value, lang } };
                            }
                        })
                        .Concat(inverseProps.ContainsKey(id) ?
                            inverseProps[id].Select(ip => new object[] { 3, new object[] { ip.pred, ip.source } }) :
                            Enumerable.Empty<InverseLink[]>())
                        .ToArray()
                    };
                return orecord;
            }).Where(r => r != null);
            records.Load(flow);
            GC.Collect();
        }

        // Используется для решения отношения naming
        private object ConvertNaming(object oo)
        {
            string tp = (string)((object[])oo)[1];
            if (tp == "http://fogid.net/o/naming")
            {
                var referred_prop = ((object[])((object[])oo)[2])
                    .Where(opr => (int)((object[])opr)[0] == 2)
                    .Select(opr => ((object[])opr)[1])
                    .FirstOrDefault(pr => (string)(((object[])pr)[0]) == "http://fogid.net/o/referred-sys");
                if (referred_prop != null)
                {
                    string idd = (string)((object[])referred_prop)[1];
                    oo = records.GetByKey(idd);
                }
            }
            return oo;
        }
        public override IEnumerable<XElement> SearchByName(string searchstring)
        {
            var qu1 = records.GetAllByLike(0, searchstring)
                .Select(r => ConvertNaming(r))
                .Distinct<object>(rSame);
            var qu = qu1
                .Select(r => ORecToXRec((object[])r, false))
                ;
            return qu;
        }
        public override IEnumerable<XElement> SearchByWords(string line)
        {
            string[] wrds = line.ToUpper().Split(delimeters);
            var qqq = wrds.SelectMany(w =>
            {
                string wrd = w;
                if (Normalize != null)
                {
                    wrd = Normalize(w);
                }
                var qu = records.GetAllByValue(1, wrd).Select(r => new { obj = r, wrd = wrd })
                    .ToArray();
                return qu;
            })
                .GroupBy(ow => (string)((object[])ow.obj)[0])
                .Select(gr => new { key = gr.Key, c = gr.Count(), o = gr.First() })
                .OrderByDescending(tri => tri.c)
                //.Take(20)
                //.ToArray()
                ;
            var query = qqq.Select(tri => tri.o.obj)
                .Distinct<object>(rSame)
                .Select(r =>
                ORecToXRec((object[])r, false));
            return query;
        }

        public override XElement GetItemByIdBasic(string id, bool addinverse)
        {
            object rec = GetRecord(id, addinverse);

            if (rec == null || Isnull(rec)) return null;
            XElement xres = ORecToXRec((object[])rec, addinverse);

            return xres;
        }
        public override object GetRecord(string id, bool addinverse)
        {
            object rec;
            rec = records.GetByKey(id);
            return rec;
        }
        public XElement ORecToXRec(object[] ob, bool addinverse)
        {
            return new XElement("record",
                new XAttribute("id", ob[0]), new XAttribute("type", ob[1]),
                ((object[])ob[2]).Cast<object[]>().Select(uni =>
                {
                    if ((int)uni[0] == 1)
                    {
                        object[] p = (object[])uni[1];
                        XAttribute langatt = string.IsNullOrEmpty((string)p[2]) ? null : new XAttribute(ONames.xmllang, p[2]);
                        return new XElement("field", new XAttribute("prop", p[0]), langatt,
                            p[1]);
                    }
                    else if ((int)uni[0] == 2)
                    {
                        object[] p = (object[])uni[1];
                        return new XElement("direct", new XAttribute("prop", p[0]),
                            new XElement("record", new XAttribute("id", p[1])));
                    }
                    else if ((int)uni[0] == 3)
                    {
                        object[] p = (object[])uni[1];
                        return new XElement("inverse", new XAttribute("prop", p[0]),
                            new XElement("record", new XAttribute("id", p[1])));
                    }
                    return null;
                }));
        }
        private object[] XRecToORec(XElement xrec)
        {
            string? id = xrec.Attribute(ONames.rdfabout)?.Value;
            if (id == null) return new object[0];
            object[] orec = new object[]
            {
                id,
                xrec.Name.NamespaceName + xrec.Name.LocalName,
                xrec.Elements()
                    .Select<XElement, object>(el =>
                    {
                        string prop = el.Name.NamespaceName + el.Name.LocalName;
                        string? resource = el.Attribute(ONames.rdfresource)?.Value;
                        if (resource == null)
                        {  // Поле
                            string? lang = el.Attribute(ONames.xmllang)?.Value;
                            return new object[] { 1, new object?[] { prop, el.Value, lang } };
                        }
                        else
                        {  // Объектная ссылка
                            return new object[] { 2, new object[] { prop, resource } };
                        }
                    }).ToArray()
            };
            return orec;
        }


        public override XElement GetItemById(string id, XElement format)
        {
            return ItemByRecord((object[])GetRecord(id, true), format);
        }
        private XElement ItemByRecord(object[] record, XElement format)
        {
            var xprops = format.Elements().SelectMany(fe =>
            {
                string? prop = fe.Attribute("prop")?.Value;
                if (fe.Name.LocalName == "field")
                {
                    var ofields = ((object[])record[2]).Cast<object[]>().Where(pa => (int)pa[0] == 1)
                        .Select(pa => (object[])pa[1]).Where(ofield => (string)ofield[0] == prop)
                        .Select(ofield => new XElement("field", new XAttribute("prop", prop),
                            (ofield[2] == null ? null : new XAttribute(ONames.xmllang, ofield[2])),
                            ofield[1]));
                    return ofields;
                }
                else if (fe.Name.LocalName == "direct")
                {
                    // Прямая ссылка может быть только одна (???)
                    var pa_prop = ((object[])record[2]).Cast<object[]>()
                        .FirstOrDefault(pa => (int)pa[0] == 2 && (string)((object[])pa[1])[0] == prop);

                    if (pa_prop == null || pa_prop.Length != 2) return Enumerable.Empty<XElement>();
                    // Найдем запись 
                    object[] drec = (object[])GetRecord((string)((object[])pa_prop[1])[1], false);
                    if (drec == null) return Enumerable.Empty<XElement>();
                    string tp = (string)drec[1];
                    XElement? f = fe.Elements("record")
                        .FirstOrDefault(xre => xre.Attribute("type") == null || xre.Attribute("type")?.Value == tp);
                    // Вот это место надо бы сделать с учетом наследования типов, а пока - упрощенный вариант
                    if (f == null)
                    {
                        XElement? ff = fe.Element("record");
                        if (ff == null) return Enumerable.Empty<XElement>();
                        f = ff;
                    }

                    // Мы подобрали формат к записи и можем рекурсивно применить метод
                    return Enumerable.Repeat<XElement>(new XElement("direct", new XAttribute("prop", prop), ItemByRecord(drec, f)), 1);
                }
                else if (fe.Name.LocalName == "inverse")
                {
                    // обратных ссылок может быть несколько (!)
                    var pa_props = ((object[])record[2]).Cast<object[]>()
                        .Where(pa => (int)pa[0] == 3 && (string)((object[])pa[1])[0] == prop);

                    XElement? f = fe.Element("record");

                    var iprops = pa_props.Select(pa => ((object[])pa[1]))
                        .Select(prop_resource => new XElement("inverse", new XAttribute("prop", prop_resource[0]),
                        ItemByRecord((object[])GetRecord((string)prop_resource[1], false), f)));
                    return iprops;
                }
                return Enumerable.Empty<XElement>();
            });
            XElement xres = new XElement("record", new XAttribute("id", (string)record[0]),
                new XAttribute("type", (string)record[1]),
                xprops,
                null);
            return xres;
        }

        public override IEnumerable<XElement> GetAll()
        {
            var query = records.ElementValues()
                .Select(record => ORecToXRec((object[])record, false));
            return query;
        }

        public override XElement Delete(string id)
        {
            throw new NotImplementedException();
        }

        public XElement? PutItem(object[] nrec)
        {
            DirectPropComparer pcomparer = new DirectPropComparer();

            // Вычисляем старую запись в объектном представлении. Ее или нет, или она в динамическом наборе или она в статическом
            // Дополнительно устанавливаем признак isindyndic
            //bool isindyndic = false;
            if (nrec.Length == 0) return null;
            var orec = GetRecord((string)nrec[0], false);
            //if (dyndic.TryGetValue(id, out orec)) { isindyndic = true; }
            //else
            //{  // или объект или null
            //    orec = records.GetByKey(new object[] { id, null, null });
            //}

            // Соберем прямые ссылки из nrec и orec (O и N) в три множества: Те которые были removed, те которые появляются appeared
            // и остальные (сохраняемые). removed = O \ N, appeared = N \ O.
            // Делаем множества пар свойство-ссылка: 
            object[] O = orec == null ? new object[0] : ((object[])((object[])orec)[2])
                .Where(x => (int)((object[])x)[0] == 2)
                .Select(x => (object[])((object[])x)[1])
                .Distinct(pcomparer)
                .ToArray();
            object[] N = ((object[])((object[])nrec)[2])
                .Where(x => (int)((object[])x)[0] == 2)
                .Select(x => (object[])((object[])x)[1])
                .Distinct(pcomparer)
                .ToArray();
            var removed = O.Except(N, pcomparer).ToArray();
            var appeared = N.Except(O, pcomparer).ToArray();

            // Если orec не нулл, перенесем из него обратные ссылки (!)
            if (orec != null)
            {
                ((object[])nrec)[2] = ((object[])((object[])nrec)[2]).Concat(((object[])(((object[])orec)[2]))
                    .Where(rprop => (int)((object[])rprop)[0] == 3))
                    .ToArray();
            }

            //// Новым значением заместим старое в динамическом наборе
            //if (isindyndic)
            //{
            //    object[] oarr = (object[])orec;
            //    object[] narr = (object[])nrec;
            //    narr[0] = oarr[0];
            //    narr[1] = oarr[1];
            //    narr[2] = oarr[2];
            //}
            //else
            //{
            //    dyndic.Add(id, nrec);
            //}
            records.AppendElement(nrec);

            //// Добавим в имена
            //additional_names.OnAddItem(nrec, long.MinValue);

            // Убираем обратные ссылки
            foreach (object[] proplink in removed)
            {
                string prop = (string)proplink[0];
                string target = (string)proplink[1];

                //// В узле target надо убрать обратные ссылки пары {prop, id}
                //bool indyn = false;
                //object node;
                //if (dyndic.TryGetValue(target, out node)) indyn = true;
                //else
                //{
                //    node = records.GetByKey(new object[] { target, null, null });
                //}
                object node = records.GetByKey(target);

                if (node != null)
                {
                    ((object[])node)[2] = ((object[])((object[])node)[2]).Cast<object[]>()
                        .Where(x => (int)x[0] != 3 || (string)((object[])x[1])[1] != (string)nrec[0] || (string)((object[])x[1])[0] != prop)
                        .ToArray();
                    //if (indyn)
                    //{
                    //    // Изменения внесены, поскольку узел уже на месте
                    //}
                    //else
                    //{
                    //    dyndic.Add(target, node);
                    //}
                    records.AppendElement(node);
                }
            }

            // Добавляем обратные ссылки
            foreach (object[] proplink in appeared)
            {
                string prop = (string)proplink[0];
                string target = (string)proplink[1];
                // В узле target надо добавить обратную ссылку пары {prop, id}
                //bool indyn = false;
                object node;
                //if (dyndic.TryGetValue(target, out node)) indyn = true;
                //else
                //{
                //    node = records.GetByKey(new object[] { target, null, null });
                //}
                node = records.GetByKey(target);
                if (node != null)
                {
                    ((object[])node)[2] = ((object[])((object[])node)[2]).Cast<object[]>()
                        .Concat(new object[] { new object[] { 3, new object[] { prop, nrec[0] } } })
                        .ToArray();
                    //if (indyn)
                    //{
                    //    // Изменения внесены, поскольку узел уже на месте
                    //}
                    //else
                    //{
                    //    dyndic.Add(target, node);
                    //}
                    records.AppendElement(node);
                }
            }
            records.Flush();
            return null;
        }

        public override XElement? PutItem(XElement record)
        {
            // Вычисляем идентификатор
            string? id = record.Attribute(ONames.rdfabout)?.Value;
            if (id == null) return null;

            // Вычисляем новую запись в объектном представлении
            object[] nrec;
            if (record.Name == "delete")
            {
                nrec = new object[] { id, "delete", new object[0] };
            }
            else if (record.Name.LocalName == "substitute")
            {
                return null;
            }
            else
            {
                nrec = XRecToORec(record);
            }
            PutItem(nrec);
            records.Flush();
            return null;
        }
        private class DirectPropComparer : IEqualityComparer<object>
        {
            public new bool Equals(object? x, object? y)
            {
                if (x == null && y == null)
                    return true;
                else if (x == null || y == null)
                    return false;
                return ((string)((object[])x)[1]).Equals((string)((object[])y)[1]) &&
                    ((string)((object[])x)[0]).Equals((string)((object[])y)[0]);
            }

            public int GetHashCode(object obj)
            {
                return ((string)((object[])obj)[1]).GetHashCode();
            }
        }
        public class RRecordSame : EqualityComparer<object>
        {
            public override bool Equals(object? b1, object? b2)
            {
                if (b1 == null && b2 == null)
                    return true;
                else if (b1 == null || b2 == null)
                    return false;
                return ((string)((object[])b1)[0] ==
                        (string)((object[])b2)[0]);
            }
            public override int GetHashCode(object bx)
            {
                string hCode = (string)((object[])bx)[0];
                return hCode.GetHashCode();
            }
        }
    }
}
