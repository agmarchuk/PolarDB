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
    public class RRecordAdapter : DAdapter
    {
        // Адаптер состоит из последовательности записей, дополнительного индекса 
        public PType tp_prop;
        public PType tp_rec;
        //private PType tp_triple;
        private USequence records;
        private SVectorIndex names;
        private SVectorIndex svwords;
        private SVectorIndex inverse_index;

        private RRecordSame rSame; // носитель компаратора для RRecord-записей


        // Может быть null или будет преобразовывать слова в "нормализованные" слова
        private Func<string, string>? Normalize;
        private Func<object, bool> Isnull;
        public RRecordAdapter(Func<string, string>? normalize)
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
                false);
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
            svwords = new SVectorIndex(GenStream2, records, toWords);
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
            inverse_index = new SVectorIndex(GenStream2, records, ext_keys, false);
            inverse_index.Refresh();

            records.uindexes = new IUIndex[]
            {
                names_ind, // Для поиска по именам
                svwords,  // Для поиска по словам
                inverse_index  // Обратный индекс
            };
        }

        public void Load(IEnumerable<object> flow)
        {
            records.Clear();
            records.Load(flow);
            records.Build();
        }
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
            records.Build();
        }
        public void Refresh() { records.Refresh(); }  

        public object GetKey(string key)
        {
            return records.GetByKey(key);
        }

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
                .Distinct<object>(rSame);
            return query;
        }
        public IEnumerable<object> GetInverseRecords(string id)
        {
            return records.GetAllByValue(2, id);
        }

        public override void Close()
        {
            Console.WriteLine($"mag: RRecordAdapter Closing {DateTime.Now} {records == null} {names == null} {svwords == null}");
            records?.Close();
            Console.WriteLine($"mag: RRecordAdapter Closed {DateTime.Now}");

        }

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
            throw new NotImplementedException();
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

        public override IEnumerable<XElement> GetAll()
        {
            throw new NotImplementedException();
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
                Func<object[], string, string?> GetInvPred = (r, i) =>
                    ((object[])(r[2]))
                    .Cast<object[]>()
                    .Where(p => (int)p[0] == 2)
                    .Select(p => (object[])p[1])
                    .Where(pair => (string)pair[1] == i)
                    .Select(pair => (string)pair[0])
                    .FirstOrDefault();
                object[] result = new object[]
                {
                rec[0],
                rec[1],
                ((object[])(rec[2])).Concat(
                    inv_recs.Select(ir => new object[] { 3, new object[] { GetInvPred((object[])ir, id) ?? "nopred", ((object[])ir)[0] } })
                    ).ToArray()
                };
                return result;
            }
            return rec;
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


