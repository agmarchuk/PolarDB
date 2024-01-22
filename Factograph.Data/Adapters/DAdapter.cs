using Factograph.Data.Adapters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Factograph.Data.Adapters
{
    abstract public class DAdapter
    {
        public bool nodatabase = true; // Опасное это дело - объявлять переменную в абстрактном классе!!!
        public abstract void Init(string connectionstring);
        public abstract void Close();
        // ============== Основные методы доступа к БД =============
        public abstract IEnumerable<XElement> SearchByName(string searchstring);
        public abstract IEnumerable<XElement> SearchByWords(string searchwords);
        public abstract XElement GetItemByIdBasic(string id, bool addinverse);
        public abstract XElement GetItemById(string id, XElement format);
        //public abstract XElement GetItemByIdSpecial(string id);
        public abstract IEnumerable<XElement> GetAll();
        public abstract object GetRecord(string id, bool addinverse);
        public abstract object? GetRecord(string id); // Простая запись формата RRecord
        public abstract IEnumerable<object> GetInverseRecords(string id); // Все записи, ссылающиеся на id

        // ============== Загрузка базы данных ===============
        public abstract void StartFillDb(Action<string> turlog);
        //public abstract void LoadFromCassettesExpress(IEnumerable<string> fogfilearr, Action<string> turlog, Action<string> convertlog);
        //public abstract void FillDb(IEnumerable<FogInfo> fogflow, Action<string> turlog);
        public abstract void LoadXFlow(IEnumerable<XElement> xflow, Dictionary<string, string> orig_ids);
        public abstract void FinishFillDb(Action<string> turlog);

        // =============== Запись базы данных в файл
        public abstract void Save(string filename);


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
            // Опеределим поток элементов при сканировании фог-файлов
            IEnumerable<XElement> fogelementsflow = fogflow
                .Where(fi => fi.vid == ".fog")
                .SelectMany(fi =>
                {
                    XElement xfog = XElement.Load(fi.pth);
                    return xfog.Elements().Select(x => ConvertXElement(x)); ;
                });
            
            //Dictionary<string, string> orig = new Dictionary<string, string>();
            //LoadXFlow(fogelementsflow, orig);
            //return;


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
            GC.Collect();

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
            GC.Collect();

            // Будем формировать единый поток x-ЗАПИСЕЙ
            IEnumerable<XElement> xflow = fogelementsflow.Append(new XElement("{http://fogid.net/o/}collection", new XAttribute(ONames.rdfabout, "cassetterootcollection"), new XElement("{http://fogid.net/o/}name", "кассеты")))
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
                    }
                );

            //// Будем формировать единый поток x-ЗАПИСЕЙ
            //IEnumerable<XElement> xflow = Enumerable.Repeat<XElement>(new XElement("{http://fogid.net/o/}collection",
            //    new XAttribute(ONames.rdfabout, "cassetterootcollection"), new XElement("{http://fogid.net/o/}name", "кассеты")), 1)
            //    .Concat(
            //        fogelementsflow
            //        .Where(rec => rec.Name != ONames.fogi + "delete" && rec.Name != ONames.fogi + "substitute")
            //        .Where(rec =>
            //        {
            //            // Пропустить надо а) записи, идентификаторы которых являются ключами в
            //            // orig_ids; б) записи, не являющиеся кандидатами на дублирование
            //            // в) Записи, являюшиеся кандидатами, но не попавшие в lastDefs
            //            // г) попавшие в lastDefs такие, что отметка времени mt >= dt
            //            // (наверное достаточно ==). В этом последнем случае надо изменить вход
            //            // с id
            //            string id = rec.Attribute(ONames.rdfabout).Value;
            //            if (orig_ids.ContainsKey(id)) return false;
            //            if (candidates.Contains(id))
            //            {
            //                XAttribute mt_att = rec.Attribute("mT");
            //                DateTime mt = mt_att == null ? DateTime.MinValue : DateTime.Parse(mt_att.Value);
            //                if (lastDefs.TryGetValue(id, out DateTime dt))
            //                {
            //                    if (mt >= dt)
            //                    {
            //                        lastDefs.Remove(id);
            //                        lastDefs.Add(id, DateTime.MaxValue);
            //                        return true;
            //                    }
            //                    else return false;
            //                }
            //                else return true;
            //            }
            //            else return true;
            //        })
            //    );


            //XElement[] els = xflow.ToArray();
            //LoadXFlow(els, orig_ids);
            LoadXFlow(xflow, orig_ids);

        }


        /// <summary>
        /// Вспомогательная процедура. Если идентификатор еще не отмеченный, то отметим его, если идентификатор уже отмеченный, то поместим значение mT в словарь
        /// </summary>
        /// <param name="bitArr"></param>
        /// <param name="Hash"></param>
        /// <param name="lastDefs"></param>
        /// <param name="id"></param>
        /// <param name="mTval">отметка времени. null - минимальное время</param>
        private static void CheckAndSet(System.Collections.BitArray bitArr, Func<string, int> Hash, Dictionary<string, DateTime> lastDefs, string id, string mTval)
        {
            int code = Hash(id);
            if (bitArr.Get(code))
            {
                // Добавляем пару в словарь
                DateTime mT = DateTime.MinValue;
                if (mTval != null) { mT = DateTime.Parse(mTval); }
                if (lastDefs.TryGetValue(id, out DateTime last))
                {
                    if (mT > last)
                    {
                        lastDefs.Remove(id);
                        lastDefs.Add(id, mT);
                    }
                }
                else
                {
                    lastDefs.Add(id, mT);
                }
            }
            else
            {
                bitArr.Set(code, true);
            }
        }



        // ============== Редактирование базы данных ============= Возвращают итоговый (или исходный для Delete) вариант записи
        public abstract XElement Delete(string id);
        // // Полная (для Add) или неполная (для AddUpdate) записи. Идентификатор обязателен.
        // public abstract XElement Add(XElement record);
        // public abstract XElement AddUpdate(XElement record);
        // Заменяет предыдущие 3. Помещает запись в базу данных, если у нее нет идентификатора, то генерирует его. Возвращает зафиксированную запись
        public abstract XElement? PutItem(XElement record);

        private static string ConvertId(string id) { if (id.Contains('|')) return id.Replace("|", ""); else return id; }

        public static Func<XElement, XElement> ConvertXElement = xel =>
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

        // ================= Раздел работы по цепочкам эквивалентности ==============
        //protected Dictionary<string, ResInfo> table_ri;
        //public void InitTableRI() { table_ri = new Dictionary<string, ResInfo>(); count_delete = count_substitute = 0; }
        //public int count_delete = 0, count_substitute = 0;
        //public void AppendXflowToRiTable(IEnumerable<XElement> xflow, string ff, Action<string> err)
        //{
        //    foreach (XElement xelement in xflow)
        //    {
        //        if (xelement.Name == ONames.fogi + "delete")
        //        {
        //            count_delete++;
        //            XAttribute att = xelement.Attribute("id");
        //            if (att == null) continue;
        //            string id = att.Value;
        //            if (id == "") continue;
        //            if (table_ri.ContainsKey(id))
        //            {
        //                var ri = table_ri[id];
        //                if (!ri.removed) // Если признак уже есть, то действия уже произведены
        //                {
        //                    // проверим, что уничтожается оригинал цепочки
        //                    if (!(id == ri.id))
        //                    {
        //                        err("Уничтожается не оригинал цепочки. fog=" +
        //                            ff + " id=" + id);
        //                    }
        //                    else ri.removed = true;
        //                }
        //            }
        //            else
        //            {
        //                table_ri.Add(id, new ResInfo(id) { removed = true });
        //            }
        //        }
        //        else if (xelement.Name == ONames.fogi + "substitute")
        //        {
        //            count_substitute++;
        //            XAttribute att_old = xelement.Attribute("old-id");
        //            XAttribute att_new = xelement.Attribute("new-id");
        //            if (att_old == null || att_new == null) continue;
        //            string id_old = att_old.Value;
        //            string id_new = att_new.Value;
        //            if (id_old == "" || id_new == "") continue;

        //            // Добудем старый и новый ресурсы
        //            //ResInfo old_res, new_res;
        //            if (!table_ri.TryGetValue(id_old, out ResInfo old_res))
        //            {
        //                old_res = new ResInfo(id_old);
        //                table_ri.Add(id_old, old_res);
        //            }
        //            if (!table_ri.TryGetValue(id_new, out ResInfo new_res))
        //            {
        //                new_res = new ResInfo(id_new);
        //                table_ri.Add(id_new, new_res);
        //            }
        //            // Проверим, что old-id совпадает с оригиналом локальной цепочки
        //            if (id_old != old_res.id)
        //            {
        //                //LogFile.WriteLine("Разветвление на идентификаторе: " + id_old);
        //            }
        //            // Перенесем тип из старой цепочки в новый ресурс
        //            if (new_res.typeid == null) new_res.typeid = old_res.typeid;
        //            else
        //            {
        //                // Проверим, что цепочки одинакового типа
        //                if (old_res.typeid != null && old_res.typeid != new_res.typeid)
        //                {
        //                    err("Err: сливаются цепочки разных типов");
        //                }
        //            }

        //            // добавляем список слитых старых идентификаторов в новый ресурс
        //            new_res.merged_ids.AddRange(old_res.merged_ids);
        //            // пробегаем по списку старых идентификаторов и перекидываем ссылку на новый ресурс
        //            foreach (string oid in old_res.merged_ids) table_ri[oid] = new_res;
        //            // перекидываем признак removed из старого ресурса, если он там true.
        //            if (old_res.removed)
        //            {
        //                // Похоже, следующий оператор ошибка. Мы "протягиваем" условие removed
        //                //new_res.removed = true;
        //                err("Протяжка удаления по цепочке. id=" + id_old);
        //            }
        //        }
        //        else
        //        {
        //            XAttribute idAtt = xelement.Attribute(ONames.rdfabout);
        //            if (idAtt == null) continue;
        //            string id = idAtt.Value;
        //            if (id == "") continue;

        //            // 
        //            if (table_ri.ContainsKey(id))
        //            {
        //                var ri = table_ri[id];
        //                DateTime modificationTime = DateTime.MinValue;
        //                XAttribute mt = xelement.Attribute("mT");
        //                if (mt != null &&
        //                    DateTime.TryParse(mt.Value, out modificationTime) &&
        //                    modificationTime.ToUniversalTime() > ri.timestamp)
        //                {
        //                    // Установим эту временную отметку
        //                    ri.timestamp = modificationTime.ToUniversalTime();
        //                }
        //                if (ri.typeid == null) ri.typeid = xelement.Name.NamespaceName + xelement.Name.LocalName;
        //                else
        //                {
        //                    // проверка на одинаковость типов
        //                    if (xelement.Name.NamespaceName + xelement.Name.LocalName != ri.typeid)
        //                    {
        //                        err("Err: тип " + xelement.Name + " для ресурса " + idAtt + " не соответствует ранее определенному типу");
        //                    }
        //                }
        //            }
        //            else
        //            { // Это вариант, когда входа еще нет в таблице
        //                DateTime modificationTime = DateTime.MinValue;
        //                XAttribute mt = xelement.Attribute("mT");
        //                if (mt != null)
        //                    DateTime.TryParse(mt.Value, out modificationTime);
        //                var n_resinfo = new ResInfo(id)
        //                {
        //                    removed = false,
        //                    timestamp = modificationTime.ToUniversalTime()
        //                    //, typeid = xelement.Name.NamespaceName + xelement.Name.LocalName
        //                };
        //                n_resinfo.typeid = xelement.Name.NamespaceName + xelement.Name.LocalName;
        //                table_ri.Add(id, n_resinfo);
        //            }

        //        }
        //    }
        //}
        //public abstract void LoadXFlowUsingRiTable(IEnumerable<XElement> xflow);


    }
    public class ResInfo
    {
        public ResInfo(string id) { this.id = id; this.merged_ids = new List<string> { id }; }
        public string id; // этот идентификатор и есть последний
        public string typeid; // тип ресурса (сущности, записи) или null если не известно
        public bool removed = false;
        public List<string> merged_ids;
        public DateTime timestamp = DateTime.MinValue;
        // Это поле вводится для того, чтобы повторно не обрабатывать определение записи
        public bool processed = false;
    }

}
