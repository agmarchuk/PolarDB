using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using System;
using Factograph.Data;

namespace Factograph.Data.r
{
    // ========== Rec классы ==========
    public class Rec
    {
        public string? Id { get; private set; }
        public string Tp { get; private set; }
        public Pro[] Props { get; internal set; }
        public Rec(string? id, string tp, params Pro[] props)
        {
            this.Id = id;
            this.Tp = tp;
            this.Props = props;
        }
        // =================== Самое главное: генерация дерева по шаблону ==========
        public static Rec Build(RRecord r, Rec shablon, IOntology ontology, Func<string, RRecord?> getRecord)
        {
            if (r == null) return new Rec("noname", "notype");
            Rec result = new(r.Id, r.Tp);
            // Следуем шаблону. Подсчитаем количество стрелок
            int[] nprops = Enumerable.Repeat<int>(0, shablon.Props.Length)
                .ToArray();
            // Другой массив говорит к какому свойству шаблона относится i-й элемент
            int[] nominshab = Enumerable.Repeat<int>(-1, r.Props.Length).ToArray();
            for (int i = 0; i < r.Props.Length; i++)
            {
                RProperty p = r.Props[i];
                int nom = -1;
                IEnumerable<Tuple<int, Pro>> pairs = Enumerable.Range(0, shablon.Props.Length)
                        .Select(i => new Tuple<int, Pro>(i, shablon.Props[i]))
                        .Where(pa =>
                            pa.Item2.Pred != null &&
                            pa.Item2.Pred == p.Prop);
                if (p is RField)
                {
                    var pair = pairs.FirstOrDefault(pa => pa.Item2 is Str || pa.Item2 is Tex);
                    if (pair != null) nom = pair.Item1;
                }
                else if (p is RLink)
                {
                    var pair = pairs.FirstOrDefault(pa => pa.Item2 is Dir);
                    if (pair != null) nom = pair.Item1;
                }
                else if (p is RInverseLink)
                {
                    var pair = pairs.FirstOrDefault(pa => pa.Item2 is Inv);
                    if (pair != null) nom = pair.Item1;
                }
                else throw new Exception("sfwefg");
                if (nom != -1)
                {
                    nprops[nom] += 1;
                    nominshab[i] = nom;
                }
            }
            // Теперь заполним свойства для результата
            Pro[] pros = new Pro[nprops.Length];
            // Обработаем все номера, и которые нулевые и которые не нулевые в nprops
            for (int j = 0; j < pros.Length; j++)
            {
                //if (nprops[j] == 0) continue;
                var p = shablon.Props[j];
                if (p is Str) pros[j] = new Str(p.Pred, null);
                else if (p is Tex) pros[j] = new Tex(p.Pred, new TextLan[nprops[j]]);
                else if (p is Dir) pros[j] = new Dir(p.Pred, new Rec[nprops[j]]);
                else if (p is Inv) pros[j] = new Inv(p.Pred, new Rec[nprops[j]]);
                else new Exception("928439");
            }
            // Сделаем массив индексов (можно было бы использовать nprops)
            int[] pos = new int[nprops.Length]; // вроде размечается нулями...
            // Снова пройдемся по свойствам записи и "разбросаем" элементы по приготовленным ячейкам.
            for (int i = 0; i < r.Props.Length; i++)
            {
                RProperty p = r.Props[i];
                // Номер в шаблоне берем из nominshab
                int nom = nominshab[i];
                // Если нет в шаблоне, то не рассматриваем
                if (nom == -1) continue;
                // Выясняем какой тип у свойства и в зависимости от типа делаем пополнение
                if (pros[nom] is Str)
                {
                    if (((Str)pros[nom]).Value == null)
                    { // нормально
                        ((Str)pros[nom]).Value = ((RField)p).Value;
                    }
                    else
                    {
                        // Бывает, что и есть, тогда просто пропустим другие значения
                        //throw new Exception($"Err: too many string values for {((RField)p).Prop}");
                    }

                }
                else if (pros[nom] is Tex)
                {
                    var f = (RField)p;
                    ((Tex)pros[nom]).Values[pos[nom]] = new TextLan(f.Value, string.IsNullOrEmpty(f.Lang) ? "ru" : f.Lang);
                    pos[nom]++;
                }
                else if (pros[nom] is Dir)
                {
                    string id1 = ((RLink)p).Resource;
                    RRecord? r1 = getRecord(id1);
                    var shablon1 = ((Dir)shablon.Props[nom]).Resources
                        .FirstOrDefault(res => res.Tp == null ||
                            (r1 != null && ontology.DescendantsAndSelf(res.Tp).Any(t => t == r1.Tp))); //res.Tp == r1?.Tp
                    if (shablon1 != null)
                    {
                        Rec r11 = Rec.Build(r1, shablon1, ontology, getRecord);
                        ((Dir)pros[nom]).Resources[pos[nom]] = r11;
                        pos[nom]++;
                    }
                    //TODO: ВОзможно, надо что-то сделать и по else
                    //else Console.WriteLine($"shablon=null {pros[nom].Pred} {r1?.Tp} {((Dir)shablon.Props[nom]).ToString()}");
                }
                else if (pros[nom] is Inv)
                {
                    string id1 = ((RInverseLink)p).Source;
                    RRecord? r1 = getRecord(id1);
                    var shablon1 = ((Inv)shablon.Props[nom]).Sources
                        .FirstOrDefault(res => res.Tp == r1?.Tp);
                    if (shablon1 != null)
                    {
                        Rec r11 = Rec.Build(r1, shablon1, ontology, getRecord);
                        ((Inv)pros[nom]).Sources[pos[nom]] = r11;
                        pos[nom]++;
                    }
                }
            }
            // Добавляем pros, устранив нулевые
            result.Props = pros.Where(p => p != null).ToArray();
            return result;
        }
        public static Rec BuildByObj(object[] r, Rec shablon, Func<string, object> getRecord)
        {
            if (r == null) return new Rec("noname", "notype");
            Rec result = new(r[0].ToString(), r[1].ToString());
            // Следуем шаблону. Подсчитаем количество стрелок
            int[] nprops = Enumerable.Repeat<int>(0, shablon.Props.Length)
                .ToArray();
            // Другой массив говорит к какому свойству шаблона относится i-й элемент
            object[] props = (object[])r[2];
            int[] nominshab = Enumerable.Repeat<int>(-1, props.Length).ToArray();
            for (int i = 0; i < props.Length; i++)
            {
                object[] p = (object[])props[i];
                string Pprop = (string)((object[])p[1])[0];
                int nom = -1;
                IEnumerable<Tuple<int, Pro>> pairs = Enumerable.Range(0, shablon.Props.Length)
                        .Select(i => new Tuple<int, Pro>(i, shablon.Props[i]))
                        .Where(pa =>
                            pa.Item2.Pred != null &&
                            pa.Item2.Pred == Pprop);
                if ((int)p[0] == 1)
                {
                    var pair = pairs.FirstOrDefault(pa => pa.Item2 is Str || pa.Item2 is Tex);
                    if (pair != null) nom = pair.Item1;
                }
                else if ((int)p[0] == 2)
                {
                    var pair = pairs.FirstOrDefault(pa => pa.Item2 is Dir);
                    if (pair != null) nom = pair.Item1;
                }
                else if ((int)p[0] == 3)
                {
                    var pair = pairs.FirstOrDefault(pa => pa.Item2 is Inv);
                    if (pair != null) nom = pair.Item1;
                }
                else throw new Exception("sfwefg");
                if (nom != -1)
                {
                    nprops[nom] += 1;
                    nominshab[i] = nom;
                }
            }
            // Теперь заполним свойства для результата
            Pro[] pros = new Pro[nprops.Length];
            // Обработаем все номера, и которые нулевые и которые не нулевые в nprops
            for (int j = 0; j < pros.Length; j++)
            {

                //if (nprops[j] == 0) continue;
                var p = shablon.Props[j];
                if (p is Str) pros[j] = new Str(p.Pred, null);
                else if (p is Tex) pros[j] = new Tex(p.Pred, new TextLan[nprops[j]]);
                else if (p is Dir) pros[j] = new Dir(p.Pred, new Rec[nprops[j]]);
                else if (p is Inv) pros[j] = new Inv(p.Pred, new Rec[nprops[j]]);
                else new Exception("928439");
            }
            // Сделаем массив индексов (можно было бы использовать nprops)
            int[] pos = new int[nprops.Length]; // вроде размечается нулями...
            // Снова пройдемся по свойствам записи и "разбросаем" элементы по приготовленным ячейкам.
            for (int i = 0; i < props.Length; i++)
            {
                object[] p = (object[])props[i];
                // Номер в шаблоне берем из nominshab
                int nom = nominshab[i];
                // Если нет в шаблоне, то не рассматриваем
                if (nom == -1) continue;
                // Выясняем какой тип у свойства и в зависимости от типа делаем пополнение
                string Pprop = (string)((object[])p[1])[0];
                string Pvalue = (string)((object[])p[1])[1];

                if (pros[nom] is Str)
                {
                    if (((Str)pros[nom]).Value == null)
                    { // нормально
                        ((Str)pros[nom]).Value = Pvalue;
                    }
                    else throw new Exception($"Err: too many string values for {Pvalue}");
                }
                else if (pros[nom] is Tex)
                {
                    string Plang = (string)((object[])p[1])[2];
                    ((Tex)pros[nom]).Values[pos[nom]] = new TextLan(Pvalue, Plang);
                    pos[nom]++;
                }
                else if (pros[nom] is Dir)
                {
                    string id1 = Pvalue;
                    object[]? r1 = (object[])getRecord(id1);
                    var shablon1 = ((Dir)shablon.Props[nom]).Resources
                        .FirstOrDefault(/*res => res.Tp == r1?[1].ToString()*/);
                    if (shablon1 != null)
                    {
                        Rec r11 = Rec.BuildByObj(r1, shablon1, getRecord);
                        ((Dir)pros[nom]).Resources[pos[nom]] = r11;
                        pos[nom]++;
                    }
                }
                else if (pros[nom] is Inv)
                {
                    string id1 = Pvalue;
                    object[]? r1 = (object[])getRecord(id1);
                    var shablon1 = ((Inv)shablon.Props[nom]).Sources
                        .FirstOrDefault(/*res => res.Tp == r1?[1].ToString()*/);
                    if (shablon1 != null)
                    {
                        Rec r11 = Rec.BuildByObj(r1, shablon1, getRecord);
                        ((Inv)pros[nom]).Sources[pos[nom]] = r11;
                        pos[nom]++;
                    }
                }
            }
            // Добавляем pros, устранив нулевые
            result.Props = pros.Where(p => p != null).ToArray();
            return result;
        }
        // Генерация универсального шаблона
        public static Rec GetUniShablon(string ty, int level, string? forbidden, IOntology ontology)
        {
            // Все прямые возможнные свойства
            string[] dprops = ontology.GetDirectPropsByType(ty).ToArray();
            var propsdirect = dprops.Select<string, Pro?>(pid =>
            {
                var os = ontology.OntoSpec
                    .FirstOrDefault(o => o.Id == pid);
                if (os == null) return null;
                if (os.Tp == "DatatypeProperty")
                {
                    var tt = ontology.RangesOfProp(pid).FirstOrDefault();
                    bool istext = tt == "http://fogid.net/o/text" ? true : false;
                    if (istext) return new Tex(pid);
                    else return new Str(pid);
                }
                else if (os.Tp == "ObjectProperty" && level > 0 && os.Id != forbidden)
                {
                    var tt = ontology.RangesOfProp(pid).FirstOrDefault();
                    if (tt == null) return null;
                    return new Dir(pid, new Rec[] { GetUniShablon(tt, 0, null, ontology) }); // Укорачивает развертку шаблона
                }
                return null;
            }).ToArray();
            string[] iprops = level > 1 ? ontology.GetInversePropsByType(ty).ToArray() : new string[0];
            var propsinverse = iprops.Select<string, Pro?>(pid =>
            {
                var os = ontology.OntoSpec
                    .FirstOrDefault(o => o.Id == pid);
                if (os == null) return null;
                if (os.Tp == "ObjectProperty")
                {
                    string[] tps = ontology.DomainsOfProp(pid).ToArray();
                    if (tps.Length == 0) return null;
                    return new Inv(pid, tps.Select(t => GetUniShablon(t, level - 1, pid, ontology)).ToArray());
                }
                return null;
            }).ToArray();
            var shab = new Rec(null, ty,
                propsdirect
                .Concat(propsinverse)
                .Where(p => p != null)
                .Cast<Pro>()
                .ToArray());
            return shab;
        }

        public static Rec? MkRec(string? id, Func<string?, bool, RRecord> getRRecord, IOntology ontology)
        {
            if (id == null) return null;
            RRecord? rrec = getRRecord(id, true);
            if (rrec == null) { return null; }
            Rec shablon;
            shablon = Rec.GetUniShablon(rrec.Tp, 2, null, ontology);
            Rec tr = Rec.Build(rrec, shablon, ontology, idd => getRRecord(idd, false));
            return tr;
        }

        // ======= Теперь доступы =======
        public string? GetStr(string pred)
        {
            var group = Props.FirstOrDefault(p => p.Pred == pred);
            if (group == null || !(group is Str)) return null;
            Str str = (Str)group;
            return str.Value;
        }
        public string? GetText(string pred)
        {
            var group = Props.FirstOrDefault(p => p.Pred == pred);
            if (group == null || !(group is Tex)) return null;
            Tex texts = (Tex)group;
            string? result = null;
            foreach (var t in texts.Values)
            {
                result = t.Text;
                if (t.Lang == "ru") break;
            }
            return result;
        }


        public static object[] RecToObject(Rec rec)
        {
            object[] orec = new object[3];
            orec[0] = rec.Id;
            orec[1] = rec.Tp;
            List<object> fields = new List<object>();
            foreach (Str str in rec.Props.Where(gr => gr is Str))
            {
                if (str.Value != null)
                {
                    object[] group = new object[2];
                    group[0] = 1;
                    object[] field = new object[3];
                    field[0] = str.Pred;
                    field[1] = str.Value;// == null ? "" : value.Text;
                    field[2] = null;// == null ? "" : value.Lang;
                    group[1] = field;
                    fields.Add(group);
                }

            }
            foreach (Tex tex in rec.Props.Where(gr => gr is Tex))
            {

                foreach (var value in tex.Values)
                {
                    if (value.Text == null || value.Text == "")
                    {
                        continue;
                    }
                    object[] group = new object[2];
                    group[0] = 1;
                    object[] field = new object[3];
                    field[0] = tex.Pred;
                    field[1] = value.Text;// == null ? "" : value.Text;
                    field[2] = value.Lang;// == null ? "" : value.Lang;
                    group[1] = field;
                    fields.Add(group);
                }
            }
            foreach (Dir dir in rec.Props.Where(gr => gr is Dir))
            {
                if (dir.Resources.Length != 0)
                {
                    fields.Add(new object[2] { 2, new object[2] { dir.Pred, dir.Resources.FirstOrDefault().Id } });
                }
            }
            orec[2] = fields.ToArray();
            return orec;
        }
        string[] months = new string[] { "янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек" };
        private string SmartDate(string? date)
        {
            if (string.IsNullOrEmpty(date)) return "";
            string year = date.Length >= 4 ? date.Substring(0, 4) : "";
            string month = date.Length >= 7 ? date.Substring(5, 2) : "";
            string day = date.Length >= 10 ? date.Substring(8, 2) : "";
            string dt = year;
            if (!string.IsNullOrEmpty(month))
            {
                int imonth;
                if (Int32.TryParse(month, out imonth) && 0 < imonth && imonth <= 12)
                {
                    dt = dt + months[imonth - 1];
                    if (!string.IsNullOrEmpty(day)) dt += day;
                }
            }
            return dt;
        }
        public string GetDates()
        {

            string? df = GetStr("http://fogid.net/o/from-date");
            string? dt = GetStr("http://fogid.net/o/to-date");
            return SmartDate(df) + (string.IsNullOrEmpty(dt) ? "" : "-" + SmartDate(dt));
        }
        public Rec? GetDirect(string pred)
        {
            var group = Props.FirstOrDefault(p => p is Dir && p.Pred == pred);
            if (group == null) return null;
            Dir dir = (Dir)group;
            if (dir.Resources.Length == 0) return null;
            return dir.Resources[0];
        }
        public Rec[] GetInverse(string pred)
        {
            var group = Props.FirstOrDefault(p => p is Inv && p.Pred == pred);
            if (group == null) return new Rec[0];
            Inv inv = (Inv)group;
            return inv.Sources;
        }

        public static XElement RecToXML(Rec rec, string owner)
        {
            var xres = new XElement(ToXName(rec.Tp),
                (rec.Id == null ? null : new XAttribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", rec.Id)),
                rec.Props.SelectMany<Pro, XElement>(p =>
                {
                    if (p is Str && ((Str)p).Value != null)
                    {
                        return new XElement[1] { new XElement(ToXName(p.Pred), ((Str)p).Value) };
                    }
                    else if (p is Tex && ((Tex)p).Values.Length != 0)
                    {
                        XElement[] xel = new XElement[((Tex)p).Values.Length];
                        for (int i = 0; i < xel.Length; i++)
                        {
                            if (((Tex)p).Values[i].Text != null && ((Tex)p).Values[i].Text != "")
                            {
                                xel[i] = new XElement(ToXName(p.Pred), ((Tex)p).Values[i].Text,
                                    ((Tex)p).Values[i] == null ? null : new XAttribute("{http://www.w3.org/XML/1998/namespace}lang", ((Tex)p).Values[i].Lang));

                            }

                        }
                        return xel;
                    }
                    else if (p is Dir && ((Dir)p).Resources.Length != 0)
                    {

                        return new XElement[1] {
                            new XElement(ToXName(p.Pred), new XAttribute("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", ((Dir)p).Resources.FirstOrDefault().Id))
                        };

                    }
                    return new XElement[0];
                }).Where(x => x != null),
                new XAttribute("owner", owner));
            return xres;
        }

        private static XName ToXName(string name)
        {
            int pos = name.LastIndexOf('/'); //TODO: Наверное, нужны еще другие окончания пространств имен
            string localName = name.Substring(pos + 1);
            string namespaceName = pos >= 0 ? name.Substring(0, pos + 1) : "";
            return XName.Get(localName, namespaceName);
        }

    }


    public abstract class Pro
    {
        public string Pred { get; internal set; } = "";
    }
    public class Tex : Pro
    {
        public TextLan[] Values { get; set; }
        public Tex(string pred, params TextLan[] values)
        {
            this.Pred = pred;
            this.Values = values;
        }
        public TextLan? GetValue(string def_lang)
        {
            TextLan? val = null;
            foreach (TextLan v in Values)
            {
                if (v.Lang == def_lang) { val = v; break; }
                else val = v;
            }
            return val;
        }
    }
    public class Str : Pro
    {
        public string? Value { get; set; }
        public Str(string pred)
        {
            this.Pred = pred;
        }
        public Str(string pred, string? value)
        {
            this.Pred = pred;
            this.Value = value;
        }
    }
    public class Dir : Pro
    {
        public Rec[] Resources { get; set; }
        public Dir(string pred, params Rec[] resources)
        {
            Pred = pred;
            Resources = resources;
        }
    }
    public class Inv : Pro
    {
        public Rec[] Sources { get; set; }
        public Inv(string pred, params Rec[] sources)
        {
            Pred = pred;
            Sources = sources;
        }
    }
}
