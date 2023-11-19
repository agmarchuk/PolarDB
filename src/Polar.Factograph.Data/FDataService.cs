using Polar.Factograph.Data.Adapters;
using Polar.Factograph.Data;
//using RDFEngine;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Polar.Factograph.Data
{
    public class FDataService : IFDataService
    {
        //public TTreeBuilder ttreebuilder;
        //public TTreeBuilder TreeBuilder { get { return ttreebuilder; } }

        public FDataService() : this("wwwroot/") { }
        public FDataService(string path)
        {
            this.path = path;
            Console.WriteLine("mag: FDataService Constructing " + DateTime.Now);
            //path = "wwwroot/";
            Init(path);
            string ontology_path = path + "Ontology_iis-v14.xml";
            ontology = new RXOntology(ontology_path);
            if (adapter is UpiAdapter)
            {
                _tbuilder = new TRecordBuilder((UpiAdapter)this.adapter, ontology);
                //ttreebuilder = new TTreeBuilder((UpiAdapter)this.adapter, ontology);
                // var qqq = ttreebuilder.GetTTree("famwf1233_1001");
            }
        }

        public CassInfo[] Cassettes { get { return cassettes; } }
        private CassInfo[] cassettes = null;
        private FogInfo[] fogs = null;
        private DAdapter adapter = null;

        private string path;
        private XElement _xconfig = null;
        private XElement XConfig { get { return _xconfig; } }

        public IOntology ontology { get; set; }

        CassInfo[] IFDataService.Cassettes => throw new NotImplementedException();

        private TRecordBuilder _tbuilder;
        public TRecordBuilder TBuilder { get { return _tbuilder; } }

        private bool directreload = false;
        private bool initiated = false;
        private bool nodatabase = false;
        public string look = "";
        public void Init(string pth)
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Restart();
            path = pth;
            Init();
            initiated = true;
            sw.Stop();
            look = "Init duration=" + sw.ElapsedMilliseconds;
        }
        private string configfilename = "config.xml";
        private Dictionary<string, string> toNormalForm = null;
        public void Init()
        {
            // Создание словаря если есть файл zaliznyak_shortform.zip
            if (File.Exists(path + "zaliznyak_shortform.zip"))
            {
                System.IO.Compression.ZipFile.ExtractToDirectory(path + "zaliznyak_shortform.zip", path);
                var reader = new StreamReader(path + "zaliznyak_shortform.txt");
                toNormalForm = new Dictionary<string, string>();
                string line = null;
                string normal = null;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] parts = line.Split(' ');
                    normal = parts[0];
                    foreach (string w in parts)
                    {
                        if (!toNormalForm.ContainsKey(w)) toNormalForm.Add(w, normal);
                    }
                }
                reader.Close();
                File.Delete(path + "zaliznyak_shortform.txt");
            }

            XElement xconfig = XElement.Load(path + configfilename);
            _xconfig = xconfig;
            // Кассеты перечислены через элементы LoadCassette. Имена кассет в файловой системе должны сравниваться по lower case
            cassettes = xconfig.Elements("LoadCassette")
                .Select(lc =>
                {
                    string cassPath = lc.Value;
                    XAttribute write_att = lc.Attribute("write");
                    string name = cassPath.Split('/', '\\').Last();
                    return new CassInfo()
                    {
                        name = name,
                        path = cassPath,
                        writable = (write_att != null && (write_att.Value == "yes" || write_att.Value == "true"))
                    };
                })
                .ToArray();

            // PrepareFogs(xconfig); -- перенесен в Load

            // Подключение к базе данных, если задано
            string connectionstring = xconfig.Element("database")?.Attribute("connectionstring")?.Value;
            if (connectionstring != null)
            {
                string pre = connectionstring.Substring(0, connectionstring.IndexOf(':'));
                Func<string, string>? NormalizeWord = null;
                if (toNormalForm != null)
                {
                    NormalizeWord = (w) =>
                    {
                        string wrd;
                        if (toNormalForm.TryGetValue(w, out wrd)) return wrd;
                        return w;
                    };
                }
                if (pre == "xml")
                {
                    adapter = new XmlDbAdapter(NormalizeWord);
                }
                else if (pre == "om")
                {
                    //adapter = new OmAdapter();
                }
                else if (pre == "uni")
                {
                    //adapter = new UniAdapter();
                }
                else if (pre == "upi")
                {
                    adapter = new UpiAdapter(NormalizeWord);
                }
                adapter.Init(connectionstring);
                PrepareFogs(XConfig);

                if (adapter.nodatabase) nodatabase = true;
                else nodatabase = false;

                if (pre == "trs" && (directreload || nodatabase)) Load();
                else if (pre == "xml") Load();
                else if (pre == "om" && (directreload || nodatabase)) Load();
                else if (pre == "uni") Load(); // Всегда загружать!
                else if (pre == "upi")
                {
                    if (directreload || nodatabase) //Load
                    {
                        Load();
                        // Надо сохранить точку записи, это делается в конце загрузки.
                        // Метод adapter.FinishFillDb(null);
                    }
                    else // Connect and restore
                    {
                        ((UpiAdapter)adapter).RestoreDynamic();
                    }
                }

                // Логфайл элементов Put()
                //putlogfilename = connectionstring.Substring(connectionstring.IndexOf(':') + 1) + "logfile_put.txt";
                putlogfilename = path + "logfile_put.txt";
            }
        }

        private void PrepareFogs(XElement xconfig)
        {
            // Формирую список фог-документов
            List<FogInfo> fogs_list = new List<FogInfo>();
            // Прямое попадание в список фогов из строчек конфигуратора
            foreach (var lf in xconfig.Elements("LoadFog"))
            {
                string fogname = lf.Value;
                int lastpoint = fogname.LastIndexOf('.');
                if (lastpoint == -1) throw new Exception("Err in fog file name construction");
                string ext = fogname.Substring(lastpoint).ToLower();
                bool writable = (lf.Attribute("writable")?.Value == "true" || lf.Attribute("write")?.Value == "yes") ?
                    true : false;
                var atts = ReadFogAttributes(fogname);
                fogs_list.Add(new FogInfo()
                {
                    vid = ext,
                    pth = fogname,
                    writable = writable && atts.prefix != null && atts.counter != null,
                    owner = atts.owner,
                    prefix = atts.prefix,
                    counter = atts.counter == null ? -1 : Int32.Parse(atts.counter)
                });

            }
            // Сбор фогов из кассет
            for (int i = 0; i < cassettes.Length; i++)
            {
                // В каждой кассете есть фог-элемент meta/имякассеты_current.fog, в нем есть владелец и может быть запрет на запись в виде
                // отсутствия атрибутов prefix или counter. Также там есть uri кассеты, надо проверить.
                CassInfo cass = cassettes[i];
                string pth = cass.path + "/meta/" + cass.name + "_current.fog";
                var atts = ReadFogAttributes(pth);
                // запишем владельца, уточним признак записи
                cass.owner = atts.owner;
                if (atts.prefix == null || atts.counter == null) cass.writable = false;
                fogs_list.Add(new FogInfo()
                {
                    //cassette = cass,
                    pth = pth,
                    fogx = null,
                    owner = atts.owner,
                    writable = true //cass.writable,
                    //prefix = atts.prefix,
                    //counter = atts.counter
                });
                // А еще в кассете могут быть другие фог-документы. Они размещаются в originals
                IEnumerable<FileInfo> fgs = (new DirectoryInfo(cass.path + "/originals"))
                    .GetDirectories("????").SelectMany(di => di.GetFiles("*.fog"));
                // Быстро проглядим документы и поместим информацию в список фогов
                foreach (FileInfo fi in fgs)
                {
                    var attts = ReadFogAttributes(fi.FullName);

                    // запишем владельца, уточним признак записи
                    //cass.owner = attts.owner;
                    fogs_list.Add(new FogInfo()
                    {
                        //cassette = cass,
                        pth = fi.FullName,
                        fogx = null,
                        owner = attts.owner,
                        //writable = cass.writable,
                        //prefix = attts.prefix,
                        //counter = attts.counter
                        writable = cass.writable && attts.prefix != null && attts.counter != null
                    }); ;
                }

            }
            // На выходе я определил, что будет массив
            fogs = fogs_list.ToArray();
            fogs_list = null;
        }

        public void Load()
        {
            adapter.StartFillDb(null);
            adapter.FillDb(fogs, null);
            adapter.FinishFillDb(null);
        }
        public void Reload()
        {
            Close();
            Init();
            Load();
        }
        private string putlogfilename = null;
        public void Close()
        {
            adapter.Close();
        }
        public void Dispose()
        {
            Close();
            Console.WriteLine("mag: FactofraphDataService Diposed " + DateTime.Now);
        }

        // ================= Заливка данными ==================
        /// <summary>
        ///  Новая версия заполнения базы данных. Сканируются XML-элементы потока, порожденного из потока фогов. 
        ///  При первом сканировании собирается словарь substitutes, задающие для некоторых идентификаторов то, 
        ///  во что они переводятся. Перевод в null обозначает уничтожение delete. Потом словарь замыкается, т.е. 
        ///  доводится до последних переопределений. В этом же проходе вычисляется множество идентификаторов -
        ///  кандидатов  на множественные определения. Это делается с использованием вспомогательного битового 
        ///  массива и псеводслучайного отображения иденификаторов на эту шкалу битов. Единичка будет означать, что
        ///  идентификаторы, попадающие в эту позицию, могут иметь множественное определение. 
        /// </summary>
        /// <param name="fogflow">Поток fog-файлов</param>
        /// <param name="turlog">лог для сообщений</param>
        public void FillDb(IEnumerable<FogInfo> fogflow, Action<string> turlog)
        {
            // Коррекция идентификаторов
            Func<string, string> ConvertId = (string id) => { if (id.Contains('|')) return id.Replace("|", ""); else return id; };

            // Коррекция элемента до канонического состояния
            Func<XElement, XElement> ConvertXElement = xel =>
            {
                if (xel.Name == "delete" || xel.Name == ONames.fogi + "delete") return new XElement(ONames.fogi + "delete",
                    xel.Attribute("id") != null ?
                        new XAttribute(ONames.rdfabout, ConvertId(xel.Attribute("id").Value)) :
                        new XAttribute(xel.Attribute(ONames.rdfabout)),
                    xel.Attribute("mT") == null ? null : new XAttribute(xel.Attribute("mT")));
                else if (xel.Name == "substitute" || xel.Name == ONames.fogi + "substitute") return new XElement(ONames.fogi + "substitute",
                    new XAttribute("old-id", ConvertId(xel.Attribute("old-id").Value)),
                    new XAttribute("new-id", ConvertId(xel.Attribute("new-id").Value)));
                else
                {
                    string id = ConvertId(xel.Attribute(ONames.rdfabout).Value);
                    XAttribute mt_att = xel.Attribute("mT");
                    XElement iisstore = xel.Element("iisstore");
                    if (iisstore != null)
                    {
                        var att_uri = iisstore.Attribute("uri");
                        var att_contenttype = iisstore.Attribute("contenttype");
                        string docmetainfo = iisstore.Attributes()
                            .Where(at => at.Name != "uri" && at.Name != "contenttype")
                            .Select(at => at.Name + ":" + at.Value.Replace(';', '|') + ";")
                            .Aggregate((sum, s) => sum + s);
                        iisstore.Remove();
                        if (att_uri != null) xel.Add(new XElement("uri", att_uri.Value));
                        if (att_contenttype != null) xel.Add(new XElement("contenttype", att_contenttype.Value));
                        if (docmetainfo != "") xel.Add(new XElement("docmetainfo", docmetainfo));
                    }
                    XElement xel1 = new XElement(XName.Get(xel.Name.LocalName, ONames.fog),
                        new XAttribute(ONames.rdfabout, ConvertId(xel.Attribute(ONames.rdfabout).Value)),
                        mt_att == null ? null : new XAttribute("mT", mt_att.Value),
                        xel.Elements()
                        .Where(sub => sub.Name.LocalName != "iisstore")
                        .Select(sub => new XElement(XName.Get(sub.Name.LocalName, ONames.fog),
                            sub.Value,
                            sub.Attributes()
                            .Select(att => att.Name == ONames.rdfresource ?
                                new XAttribute(ONames.rdfresource, ConvertId(att.Value)) :
                                new XAttribute(att)))));
                    return xel1;
                }
                //return null;
            };
            // Опеределим поток элементов при сканировании фог-файлов
            var fogelementsflow = fogflow
                    .Where(fi => fi.vid == ".fog")
                    .SelectMany(fi =>
                    {
                        XElement xfog = XElement.Load(fi.pth);
                        return xfog.Elements().Select(x => ConvertXElement(x)); ;
                    });


            Dictionary<string, string> substitutes = new Dictionary<string, string>();

            // Готовим битовый массив для отметки того, что хеш id уже "попадал" в этот бит
            int rang = 24; // пока предполагается, что число записей (много) меньше 16 млн.
            int mask = ~((-1) << rang);
            int ba_volume = mask + 1;
            System.Collections.BitArray bitArr = new System.Collections.BitArray(ba_volume);
            // Хеш-функция будет ограничена rang разрядами, массив и функция нужны только временно
            Func<string, int> Hash = s => s.GetHashCode() & mask;

            // Множество кандидатов на многократное определение
            HashSet<string> candidates = new HashSet<string>();

            // Первый проход сканирования. Строим таблицу substitutes строка-строка с соответствием идентификатору
            // идентификатора оригинала или null. Строим 
            foreach (XElement record in fogelementsflow)
            {
                // Обработаем delete и replace
                if (record.Name == ONames.fogi + "delete")
                {
                    string idd = record.Attribute(ONames.rdfabout)?.Value;
                    if (idd == null) idd = record.Attribute("id").Value;
                    if (substitutes.ContainsKey(idd))
                    {
                        substitutes.Remove(idd);
                    }
                    substitutes.Add(idd, null);
                    continue;
                }
                else if (record.Name == ONames.fogi + "substitute")
                {
                    string idold = record.Attribute("old-id").Value;
                    string idnew = record.Attribute("new-id").Value;
                    // может old-id уже уничтожен, огда ничего не менять
                    if (substitutes.TryGetValue(idold, out string value))
                    {
                        if (value == null) continue;
                        substitutes.Remove(idold);
                    }
                    // иначе заменить в любом случае
                    substitutes.Add(idold, idnew);
                    continue;
                }
                else
                {
                    string id = record.Attribute(ONames.rdfabout)?.Value;
                    if (id == null) throw new Exception("29283");
                    int code = Hash(id);
                    if (bitArr[code])
                    {
                        // Это означает, что кандидат id выявлен
                        candidates.Add(id);
                    }
                    else
                    {
                        bitArr.Set(code, true);
                    }
                }
            }

            // Функция, добирающаяся до последнего определения или это и есть последнее
            Func<string, string> original = null;
            original = id =>
            {
                if (substitutes.TryGetValue(id, out string idd))
                {
                    if (idd == null) return id;
                    return original(idd);
                }
                return id;
            };

            // Обработаем словарь, формируя новый 
            Dictionary<string, string> orig_ids = new Dictionary<string, string>();
            foreach (var pair in substitutes)
            {
                string key = pair.Key;
                string value = pair.Value;
                if (value != null) value = original(value);
                orig_ids.Add(key, value);
            }

            // Словарь, фиксирующий максимальную дату для заданного идентификатора, первая запись не учитывается
            Dictionary<string, DateTime> lastDefs = new Dictionary<string, DateTime>();
            foreach (XElement record in fogelementsflow)
            {

                // Обработаем delete и replace
                if (record.Name == ONames.fogi + "delete")
                {
                }
                else if (record.Name == ONames.fogi + "substitute")
                {
                }
                else
                {
                    string id = record.Attribute(ONames.rdfabout).Value;
                    if (candidates.Contains(id))
                    {
                        XAttribute mt_att = record.Attribute("mT");
                        DateTime mt = mt_att == null ? DateTime.MinValue : DateTime.Parse(mt_att.Value);
                        if (lastDefs.TryGetValue(id, out DateTime dt))
                        {
                            if (mt > dt)
                            {
                                lastDefs.Remove(id);
                                lastDefs.Add(id, mt);
                            }
                        }
                        else
                        {
                            lastDefs.Add(id, mt);
                        }
                    }
                }
            }

            // Будем формировать единый поток x-ЗАПИСЕЙ
            IEnumerable<XElement> xflow = Enumerable.Repeat<XElement>(new XElement("{http://fogid.net/o/}collection",
                new XAttribute(ONames.rdfabout, "cassetterootcollection"), new XElement("{http://fogid.net/o/}name", "кассеты")), 1)
                .Concat(
                    fogelementsflow
                    .Where(rec => rec.Name != ONames.fogi + "delete" && rec.Name != ONames.fogi + "substitute")
                    .Where(rec =>
                    {
                        // Пропустить надо а) записи, идентификаторы которых являются ключами в
                        // orig_ids; б) записи, не являющиеся кандидатами на дублирование
                        // в) Записи, являюшиеся кандидатами, но не попавшие в lastDefs
                        // г) попавшие в lastDefs такие, что отметка времени mt >= dt
                        // (наверное достаточно ==). В этом последнем случае надо изменить вход
                        // с id
                        string id = rec.Attribute(ONames.rdfabout).Value;
                        if (orig_ids.ContainsKey(id)) return false;
                        if (candidates.Contains(id))
                        {
                            XAttribute mt_att = rec.Attribute("mT");
                            DateTime mt = mt_att == null ? DateTime.MinValue : DateTime.Parse(mt_att.Value);
                            if (lastDefs.TryGetValue(id, out DateTime dt))
                            {
                                if (mt >= dt)
                                {
                                    lastDefs.Remove(id);
                                    lastDefs.Add(id, DateTime.MaxValue);
                                    return true;
                                }
                                else return false;
                            }
                            else return true;
                        }
                        else return true;
                    })
                );


            XElement[] els = xflow.ToArray();
            adapter.LoadXFlow(els, orig_ids);
            //LoadXFlow(xflow, orig_ids);

        }



        // ============= Доступ к документным файлам по uri и параметрам ===============
        public string CassDirPath(string uri)
        {
            if (!uri.StartsWith("iiss://")) throw new Exception("Err: 22233");
            int pos = uri.IndexOf('@', 7);
            if (pos < 8) throw new Exception("Err: 22234");
            var s1 = uri.Substring(7, pos - 7).ToLower();
            var sear = cassettes.FirstOrDefault(c => c.name.ToLower() == s1);
            return sear?.path;
        }
        public string GetFilePath(string u, string s)
        {
            if (u == null) return null;
            u = System.Web.HttpUtility.UrlDecode(u);
            var cass_dir = CassDirPath(u);
            if (cass_dir == null) return null;
            string last10 = u.Substring(u.Length - 10);
            string subpath;
            string method = s;
            if (method == null) subpath = "/originals";
            if (method == "small") subpath = "/documents/small";
            else if (method == "medium") subpath = "/documents/medium";
            else subpath = "/documents/normal"; // (method == "n")
            string path = cass_dir + subpath + last10;
            return path;
        }
        public string? GetOriginalPath(string u)
        {
            if (u == null) return null;
            u = System.Web.HttpUtility.UrlDecode(u);
            var cass_dir = CassDirPath(u);
            if (cass_dir == null) return null;
            string last10 = u.Substring(u.Length - 10);
            string dnom = last10.Substring(0, 6);
            string fnom = last10.Substring(6);
            string subpath = "/originals" + dnom;
            string path = cass_dir + subpath;

            var dinfo = new System.IO.DirectoryInfo(cass_dir + subpath);
            var qu = dinfo.GetFiles(fnom + ".*");
            if (qu.Length == 0) return null;

            return qu[0].FullName;
        }


        // Доступ ка базе данных
        public IEnumerable<XElement> SearchByName(string ss)
        {
            return adapter.SearchByName(ss);
        }
        public IEnumerable<XElement> SearchByWords(string ss)
        {
            return adapter.SearchByWords(ss);
        }
        public XElement GetItemByIdBasic(string id, bool addinverse)
        {
            var val = adapter.GetItemByIdBasic(id, addinverse);
            return val;
        }

        /// <summary>
        /// Делает портрет айтема, годный для простого преобразования в html-страницу или ее часть.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public XElement GetBasicPortrait(string id)
        {
            XElement tree = GetItemByIdBasic(id, true);
            return new XElement("record", new XAttribute(tree.Attribute("id")), new XAttribute(tree.Attribute("type")),
                tree.Elements().Where(el => el.Name == "field" || el.Name == "direct")
                .Select(el =>
                {
                    if (el.Name == "field") return new XElement(el);
                    string prop = el.Attribute("prop").Value;
                    string target = el.Element("record").Attribute("id").Value;
                    XElement tr = GetItemByIdBasic(target, false);
                    return new XElement("direct", new XAttribute("prop", prop),
                        new XElement("record",
                            new XAttribute(tr.Attribute("id")),
                            new XAttribute(tr.Attribute("type")),
                            tr.Elements()));
                }),
                null);
        }

        public XElement GetItemById(string id, XElement format)
        {
            return adapter.GetItemById(id, format);
        }
        public IEnumerable<XElement> GetAll()
        {
            return adapter.GetAll();
        }
        public XElement UpdateItem(XElement item)
        {
            string id = item.Attribute(ONames.rdfabout)?.Value;
            if (id == null)
            {  // точно не апдэйт
                return PutItem(item);
            }
            else
            { // возможно update
                XElement old = adapter.GetItemByIdBasic(id, false);
                if (old == null) return PutItem(item);
                // добавляем старые, которых нет. Особенность в том, что старые - в запросном формате, новые - в базовом. 
                IEnumerable<XElement> adding = old.Elements()
                    .Select(oe =>
                    {
                        string prop_value = oe.Attribute("prop").Value;
                        string lang = oe.Attribute("{http://www.w3.org/XML/1998/namespace}lang")?.Value;
                        bool similar = item.Elements().Where(el => el.Name.NamespaceName + el.Name.LocalName == prop_value).Any(el =>
                        {
                            string olang = el.Attribute("{http://www.w3.org/XML/1998/namespace}lang")?.Value;
                            return (lang == null && olang == null ? true : lang == olang);
                        });
                        if (similar) return (XElement)null; // Если найден похожий, то не нужен старый
                        else
                        {
                            int pos = prop_value.LastIndexOf('/');
                            XName xn = XName.Get(prop_value.Substring(pos + 1), prop_value.Substring(0, pos + 1));
                            return new XElement(xn,
                                lang == null ? null : new XAttribute("{http://www.w3.org/XML/1998/namespace}lang", lang),
                                oe.Value); // Добавляем старый
                        }
                    });
                XElement nitem = new XElement(item.Name, item.Attributes(), item.Elements(), adding);
                //// новые свойства. TODO: Языковые варианты опущены!
                //XElement nitem = new XElement(item);
                //string[] props = nitem.Elements().Select(el => el.Name.LocalName).ToArray();
                //nitem.Add(old.Elements()
                //.Select(el =>
                //{
                //    string prop = el.Attribute("prop").Value;
                //    int pos = prop.LastIndexOf('/');
                //    XName subel_name = XName.Get(prop.Substring(pos + 1), prop.Substring(0, pos + 1));
                //    if (props.Contains(prop.Substring(pos + 1))) return null;
                //    XElement subel = new XElement(subel_name);
                //    if (el.Name == "field") subel.Add(el.Value);
                //    else if (el.Name == "direct") subel.Add(new XAttribute(ONames.rdfresource, el.Element("record").Attribute("id").Value));
                //    return subel;
                //}));
                return PutItem(nitem);
            }
        }
        public bool HasWritabeFogForUser(string? user)
        {
            if (user == null) return false;
            return fogs.Any(f => f.owner == user && f.writable);
        }
        public XElement PutItem(XElement item)
        {
            //XElement result = null;
            string? owner = item.Attribute("owner")?.Value;

            // Запись возможна только если есть код владельца
            if (owner == null) return new XElement("error", "no owner attribute");

            // Проверим и изменим отметку времени
            string mT = DateTime.Now.ToUniversalTime().ToString("u");
            XAttribute? mT_att = item.Attribute("mT");
            if (mT_att == null) item.Add(new XAttribute("mT", mT));
            else mT_att.Value = mT;


            // Ищем подходящий фог-документ
            FogInfo? fi = fogs.FirstOrDefault(f => f.owner == owner && f.writable);

            // Если нет подходящего - запись не производится
            if (fi == null) return new XElement("error", "no writable fog for request");

            // Если фог не загружен, то загрузить его
            if (fi.fogx == null) fi.fogx = XElement.Load(fi.pth);

            // Изымаем из пришедшего элемента владельца и фиксируем его в фоге
            XAttribute? owner_att = item.Attribute("owner");
            if (owner_att != null) owner_att.Remove();

            // Формируемый элемент (new item)
            XElement nitem;

            // substitute обрабатываем отдельно
            if (item.Name == "substitute")
            {
                nitem = item;
            }
            else
            {
                // читаем или формируем идентификатор
                string? id = item.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")?.Value;
                XElement? element = null; // запись с пришедшим идентификатором
                if (id == null)
                {
                    XAttribute? counter_att = fi.fogx.Attribute("counter");
                    XAttribute? prefix_att = fi.fogx.Attribute("prefix");
                    if (counter_att != null && prefix_att != null)
                    {
                        int counter = Int32.Parse(counter_att.Value);
                        id = prefix_att.Value + counter;
                        counter_att.Value = "" + (counter + 1);
                    }
                    else
                    {
                        Random rnd = new Random();
                        var r = rnd.Next();
                        byte[] bytes = new byte[4];
                        uint mask = 255;
                        for (int i = 0; i < 4; i++)
                        {
                            bytes[i] = (byte)(r & mask);
                            r >>= 8;
                        }
                        id = Convert.ToBase64String(bytes);
                    }
                    // внедряем
                    item.Add(new XAttribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", id));
                }
                else
                {
                    element = fi.fogx.Elements().FirstOrDefault(el =>
                        el.Attribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")?.Value == id);
                }

                if (element != null)
                {
                    element.Remove();
                }

                // Очищаем запись от пустых полей
                nitem = new XElement(item.Name, item.Attribute(ONames.rdfabout), item.Attribute("mT"),
                    item.Elements().Select(xprop =>
                    {
                        XAttribute? aresource = xprop.Attribute(ONames.rdfresource);
                        if (aresource == null)
                        {   // DatatypeProperty
                            if (string.IsNullOrEmpty(xprop.Value)) return null; // Глевное убирание!!!
                            return new XElement(xprop);
                        }
                        else
                        {   // ObjectProperty
                            return new XElement(xprop); //TODO: Возможно, надо убрать ссылки типа ""
                        }
                    }),
                    null);

            }
            fi.fogx.Add(nitem);

            // Сохраняем файл
            fi.fogx.Save(fi.pth);

            // Сохраняем в базе данных
            adapter.PutItem(nitem);

            // Сохраняем в логе
            using (Stream log = File.Open(putlogfilename, FileMode.Append, FileAccess.Write))
            {
                TextWriter tw = new StreamWriter(log, System.Text.Encoding.UTF8);
                tw.WriteLine(nitem.ToString());
                tw.Close();
            }

            return new XElement(nitem);
        }


        private static (string owner, string prefix, string counter) ReadFogAttributes(string pth)
        {
            // Нужно для чтиния в кодировке windows-1251. Нужен также Nuget System.Text.Encoding.CodePages
            var v = System.Text.CodePagesEncodingProvider.Instance;
            System.Text.Encoding.RegisterProvider(v);

            string owner = null;
            string prefix = null;
            string counter = null;
            XmlReaderSettings settings = new XmlReaderSettings();
            using (XmlReader reader = XmlReader.Create(pth, settings))
            {
                while (reader.Read())
                {
                    if (reader.NodeType == XmlNodeType.Element)
                    {
                        if (reader.Name != "rdf:RDF") throw new Exception($"Err: Name={reader.Name}");
                        owner = reader.GetAttribute("owner");
                        prefix = reader.GetAttribute("prefix");
                        counter = reader.GetAttribute("counter");
                        break;
                    }
                }
            }
            //Console.WriteLine($"ReadFogAttributes({pth}) : {owner} {prefix} {counter} ");
            return (owner, prefix, counter);
        }

        public DAdapter GetAdapter()
        {
            return adapter;
        }
    }
}
