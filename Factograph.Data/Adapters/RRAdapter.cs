using Factograph.Data;
using Factograph.Data.r;
using Polar.DB;
using Polar.Universal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using static Factograph.Data.Adapters.UpiAdapter;

namespace Factograph.Data.Adapters
{
    public class RRAdapter : DAdapter
    {
        // Адаптер состоит из последовательности записей, дополнительного индекса 
        public PType tp_prop;
        public PType tp_rec;
        //private PType tp_triple;
        private USequence records;
        public USequence GetRecordsSequence() { return records; } 
        private SVectorIndex names;
        //private SVectorIndex svwords;
        private UVecIndex svwords;
        //private SVectorIndex inverse_index;
        private UVecIndex inverse_index;

        private Func<object, IEnumerable<string>> toWords;

        private RRecordSame rSame; // носитель компаратора для RRecord-записей


        // Может быть null или будет преобразовывать слова в "нормализованные" слова
        private Func<string, string>? Normalize;
        private Func<object, bool> Isnull;
        public RRAdapter(Func<string, string>? normalize)
        {
            this.Normalize = normalize;
            tp_prop = new PTypeUnion(
                new NamedType("novariant", new PType(PTypeEnumeration.none)),
                new NamedType("field", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("value", new PType(PTypeEnumeration.sstring)),
                    new NamedType("lang", new PType(PTypeEnumeration.sstring)))),
                new NamedType("objprop", new PTypeRecord(
                    new NamedType("prop", new PType(PTypeEnumeration.sstring)),
                    new NamedType("link", new PType(PTypeEnumeration.sstring))))
                );
            tp_rec = new PTypeRecord(
                new NamedType("id", new PType(PTypeEnumeration.sstring)),
                new NamedType("tp", new PType(PTypeEnumeration.sstring)), // Признак delete будет в поле типа
                                                                          //new NamedType("deleted", new PType(PTypeEnumeration.boolean)),
                new NamedType("props", new PTypeSequence(tp_prop))
                );

            // Функция "нуля"        
            Isnull = ob => (string)((object[])ob)[1] == "delete"; // Второе поле может "обнулить" значение с этим id

            // Компаратор
            rSame = new RRecordSame();
        }

        // Главный инициализатор. Используем connectionstring 
        //bool nodatabase = false; // Есть в абстрактном классе
        private string dbfolder;
        private int file_no = 0;
        private char[] delimeters;

        public override void Init(string connectionstring)
        {
            if (connectionstring != null && connectionstring.StartsWith("rr:"))
            {
                dbfolder = connectionstring.Substring("rr:".Length);
            }
            //else dbfolder = connectionstring.Substring(4);

            this.nodatabase = false;
            if (!File.Exists(dbfolder + "0.bin"))
            {
                Console.Write("Reload db files");
                this.nodatabase = true;
            }

            // Генератор стримов
            Func<Stream> GenStream2 = () =>
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

            // Функция для ключа   Func<object, int> intId = obj => ((int)(((object[])obj)[0]));


            // Создаем последовательность записей
            records = new USequence(tp_rec, dbfolder + "state.bin", GenStream2,
                rec => (string)((object[])rec)[1] == "delete", // признак уничтоженности
                rec => (string)((object[])rec)[0], // как брать ключ
                kval => (int)Hashfunctions.HashRot13((string)kval), // как делать хеш от ключа
                true);
            records.Refresh();

            GC.Collect();

            // ====== Добавим дополнительные индексы 
            // Заведем функцию вычисления полей name и alias (поле векторное, поэтому выдаем массив)
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

            var names_ind = new SVectorIndex(GenStream2, records, skey);
            names_ind.Refresh();


            // Компаратор
            //rSame = new RRecordSame(); -- уже определял

            // Теперь индекс по словам. Нужны разделители, нужны предикаты полей, нужна функция вычисления слов
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
            toWords = obj =>
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
            svwords = new UVecIndex(GenStream2, records, toWords,
                v => Hashfunctions.HashRot13((string)v), true);
            svwords.Refresh();

            // Обратный индекс состоит из множества пар { predicate, resource } PredResourcePair (см. в конце),
            // Там же функция GetPredResourcePairs, преобразующая запись в поток пар. Пары будут 
            // упорядоченны по ресурсам и снабженных целочисленным хешем (тоже по ресурсам) - НЕ ПОЛУЧИЛОСЬ!
            //UVecIndex inverse_ind = new UVecIndex(GenStream2, records,
            //    ob => (IEnumerable<IComparable>)PredResourcePair.GetPredResourcePairs(ob),
            //    pa => Hashfunctions.HashRot13((string)((PredResourcePair)pa).Resource));

            // Другой вариант - использовать SVectorIndex. 

            // Сначала сформулируем функцию, переводящую запись-объект в набор идентификаторов сущностей
            Func<object, IEnumerable<string>> ext_keys = obj =>
            {
                object[] props = (object[])((object[])obj)[2];
                if (props.Any(p => (int)((object[])p)[0] == 2)) { }
                var query = props.Where(p => (int)((object[])p)[0] == 2)
                    .Select(p => ((object[])p)[1])
                    .Cast<object[]>()
                    .Select(p => (string)p[1]).ToArray();
                return query;
            };
            //inverse_index = new SVectorIndex(GenStream2, records, ext_keys, false);
            inverse_index = new UVecIndex(GenStream2, records, ext_keys, v => Hashfunctions.HashRot13((string)v));
            inverse_index.Refresh();

            records.uindexes = new IUIndex[]
            {
                names_ind, // Для поиска по именам
                svwords,  // Для поиска по словам
                inverse_index  // Обратный индекс
            };

            GC.Collect();
            Console.WriteLine($"==={DateTime.Now}===After Init === Total Memory: " + GC.GetTotalMemory(true));
        }

        //public void Load(IEnumerable<object> flow)
        //{
        //    records.Clear();
        //    records.Load(flow);
        //    records.Build();
        //}
        public override void LoadXFlow(IEnumerable<XElement> xflow, Dictionary<string, string> orig_ids)
        {
            records.Clear();
            records.Load(
                xflow
                .Select<XElement, object>(x =>
                    {
                        string? id = x.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")?.Value;
                        if (id == null) return null;
                        string tp = x.Name.NamespaceName + x.Name.LocalName;
                        object[] oelement = new object[]
                        {
                            id, tp, x.Elements()
                            .Select(el =>
                            {
                                string pred = el.Name.NamespaceName + el.Name.LocalName;
                                XAttribute? att = el.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource");
                                if (att != null)
                                {
                                    return new object[] { 2, new object[] { pred, att.Value } };
                                }
                                else
                                {
                                    XAttribute? lang_att = el.Attribute("{http://www.w3.org/XML/1998/namespace}lang");
                                    string? lang = lang_att == null ? null : lang_att.Value;
                                    return new object[] { 1, new object[] { pred, el.Value, lang } };
                                }
                            }).ToArray()
                        };
                        return oelement;
                    })
                .Where(ob => ob != null)
                );
            records.Flush();
            //records.Build();
            GC.Collect();
            Console.WriteLine($"==={DateTime.Now}==== After Load ===== Total Memory: " + GC.GetTotalMemory(true));
        }
        public void Refresh() { records.Refresh(); }  



        public IEnumerable<object> SearchName(string searchstring)
        {
            var qu1 = records.GetAllByLike(0, searchstring)
                .Select(r => ConvertNaming(r))
                .Distinct<object>(rSame);
            return qu1;
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
        public IEnumerable<object> SearchWords(string line)
        {
            string[] wrds = line.ToUpper().Split(delimeters);
            var qqq = wrds.SelectMany(w =>
            {
                string wrd = w;
                if (Normalize != null)
                {
                    wrd = Normalize(w);
                }
                var qu = records.GetAllByValue(1, wrd, toWords, true).Select(r => new { obj = r, wrd = wrd })
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
                .Distinct<object>(rSame);
            return query;
        }
        public override object? GetRecord(string id)
        {
            var qu = records.GetByKey(id);
            return qu;
        }


        public override void Close()
        {
            records?.Close();
            Console.WriteLine($"mag: RRAdapter Closed {DateTime.Now}");

        }

        // ============= ОБъектные интерфесы ============= 
        public object GetKey(string key)
        {
            return records.GetByKey(key);
        }
        public override IEnumerable<object> GetInverseRecords(string id)
        {
            var qu = records.GetAllByValue(2, id, ob =>
            {
                var q = ((object[])((object[])ob)[2])
                    .Where(p => (int)((object[])p)[0] == 2)
                    .Select(p => (string)((object[])((object[])p)[1])[1]);
                return q;
            }).ToArray();
            return qu;
        }
        public override object GetRecord(string id, bool addinverse)
        {
            // Получение расширенной записи в объектном представлении
            // Структура записи: три поля верхнего уровня: идентификатор, тип, свойства, свойства - последовательность свойств
            // Свойство может быть полем, прямой ссылкой, обратной ссылкой
            // Поле (тег 1): три значения - предикат, текст значения, язык
            // Прямая ссылка (тег 2): два значения - предикат, идентификатор
            // Обратная ссылка (тег 3): два значения - предикат, идентификатор источника
            object[] rec = (object[])GetKey(id);
            if (addinverse)
            {
                IEnumerable<object> inv_recs = GetInverseRecords(id).ToArray();
                //Func<object[], string, string?> GetInvPred = (r, i) =>
                //    ((object[])(r[2]))
                //    .Cast<object[]>()
                //    .Where(p => (int)p[0] == 2)
                //    .Select(p => (object[])p[1])
                //    .Where(pair => (string)pair[1] == i)
                //    .Select(pair => (string)pair[0])
                //    .FirstOrDefault();
                
                // Нам требуются множество обратных предикатов и ссылочных истоков
                // Берем множество обратных записей, приводим записи к массивам, вырабатываем поток групп свойств 
                var inv_props = inv_recs.Cast<object[]>()
                    .Select(re =>
                    {
                        var qu = ((object[])((object[])re)[2])
                            .First(p => (int)((object[])p)[0] == 2 && (string)((object[])((object[])p)[1])[1] == id);
                        return new object[] { 3, new object[] { (string)((object[])((object[])qu)[1])[0],
                                (string)((object[])re)[0] } };
                    });
                object[] result = new object[]
                {
                    rec[0],
                    rec[1],
                    ((object[])(rec[2])).Concat(inv_props).ToArray()
                };
                return result;
            }
            return rec;
        }

        // ========== XML-интерфейсы ===========
        public override IEnumerable<XElement> SearchByName(string searchstring)
        {
            return SearchName(searchstring)
                .Select(re =>
                {
                    return ORecToXRec((object[])re, false);
                });
        }

        public override IEnumerable<XElement> SearchByWords(string searchwords)
        {
            return SearchWords(searchwords)
                .Select(re =>
                {
                    return ORecToXRec((object[])re, false);
                });
        }

        public override XElement GetItemByIdBasic(string id, bool addinverse)
        {
            var rec = GetRecord(id, addinverse);
            if (rec == null || Isnull(rec)) return null;
            XElement xres = ORecToXRec((object[])rec, addinverse);
            return xres;
        }

        public override XElement GetItemById(string id, XElement format)
        {
            throw new NotImplementedException();
        }

        public override void StartFillDb(Action<string> turlog)
        {
            records.Clear();
        }

        //public override void LoadXFlow(IEnumerable<XElement> xflow, Dictionary<string, string> orig_ids)
        //{
        //    throw new NotImplementedException();
        //}

        public override void FinishFillDb(Action<string> turlog)
        {
            records.Build();
            GC.Collect();
            Console.WriteLine($"==={DateTime.Now}===After Build === Total Memory: " + GC.GetTotalMemory(true));
        }

        public override XElement Delete(string id)
        {
            throw new NotImplementedException();
        }

        public override XElement? PutItem(XElement record)
        {
            // Какой элемент изменяется?
            string? id = record.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")?.Value;
            if (id == null) throw new ArgumentNullException("id"); // return null;

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
        /// <summary>
        /// Задача состоит в том, чтобы полученную в объектном представлении запись "довести до ума"
        /// </summary>
        /// <param name="nrec"></param>
        public void PutItem(object[] nrec)
        {
            //// ======= Попытка что-то вставить...
            //DirectPropComparer pcomparer = new DirectPropComparer();

            //// Вычисляем старую запись в объектном представлении. Ее или нет, или она в динамическом наборе или она в статическом
            //if (nrec.Length == 0) return;
            //var orec = GetRecord((string)nrec[0], false);

            //// Соберем прямые ссылки из nrec и orec (O и N) в три множества: Те которые были removed, те которые появляются appeared
            //// и остальные (сохраняемые). removed = O \ N, appeared = N \ O.
            //// Делаем множества пар свойство-ссылка: 
            //object[] O = orec == null ? new object[0] : ((object[])((object[])orec)[2])
            //    .Where(x => (int)((object[])x)[0] == 2)
            //    .Select(x => (object[])((object[])x)[1])
            //    .Distinct(pcomparer)
            //    .ToArray();
            //object[] N = ((object[])((object[])nrec)[2])
            //    .Where(x => (int)((object[])x)[0] == 2)
            //    .Select(x => (object[])((object[])x)[1])
            //    .Distinct(pcomparer)
            //    .ToArray();
            //var removed = O.Except(N, pcomparer).ToArray();
            //var appeared = N.Except(O, pcomparer).ToArray();

            //// Если orec не нулл, перенесем из него обратные ссылки (!)
            //if (orec != null)
            //{
            //    ((object[])nrec)[2] = ((object[])((object[])nrec)[2]).Concat(((object[])(((object[])orec)[2]))
            //        .Where(rprop => (int)((object[])rprop)[0] == 3))
            //        .ToArray();
            //}
            //// ======= конец попытки

            records.AppendElement(nrec);
            records.Flush();
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

        public void RestoreDynamic()
        {
            // Восстановить динаические значения в индексах можно так:
            records.RestoreDynamic();
        }

        public override IEnumerable<XElement> GetAll()
        {
            var query = records.ElementValues()
                .Select(record => ORecToXRec((object[])record, false));
            return query;
        }
        public override void Save(string filename)
        {
            Func<string, XName> xn = sn =>
            {
                int pos = sn.LastIndexOf('/');
                XName xn = XName.Get(sn.Substring(pos + 1), sn.Substring(0, pos + 1));
                return xn;
            };
            XElement xall = XElement.Parse(
@"<?xml version='1.0' encoding='utf-8'?>
<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#' xmlns='http://fogid.net/o/' owner='mag4387' prefix='mag4387_' counter='1003'>
</rdf:RDF>");
            var xelements = GetAll();
            xall.Add(xelements.Select(xe =>
            {
                if (xe.Name != "record") throw new Exception("Err:kjek");
                string? id = xe.Attribute("id")?.Value;
                string? tp = xe.Attribute("type")?.Value;
                if (id == null || tp == null) throw new Exception("Err:slkf");

                XElement xres = new XElement(xn(tp), new XAttribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", id), 
                    xe.Elements().Select(xe => 
                    {
                        string? pred = xe.Attribute("prop")?.Value;
                        if (pred == null) throw new Exception("err: 2938");
                        if (xe.Name == "field")
                        {
                            return new XElement(xn(pred), xe.Value);
                        }
                        else if (xe.Name == "direct")
                        {
                            return new XElement(xn(pred), new XAttribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", xe.Element("record")?.Attribute("id")?.Value));
                        }
                        else throw new Exception("err:wiuei");
                    }));
                return xres;
            }));
            xall.Save(filename);
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


