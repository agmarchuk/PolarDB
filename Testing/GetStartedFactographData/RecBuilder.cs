using Factograph.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Resources;
using System.Security;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleTest
{
    public class RecBuilder
    {
        private Func<string, RRecord?> getRecord;

        public RecBuilder(Func<string, RRecord?> getRecord)
        {
            this.getRecord = getRecord;
        }

        public Rec? ToRec(RRecord r, Rec shablon)
        {
            if (r == null) return null;
            Rec result = new Rec(r.Id, r.Tp);
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
            // Обработаем только номера, которые не нулевые в nprops
            for (int j = 0; j < pros.Length; j++)
            {
                if (nprops[j] == 0) continue;
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
                    if (((Str)pros[i]).Value == null)
                    { // нормально
                        ((Str)pros[i]).Value = ((RField)p).Value;
                    }
                    else throw new Exception($"Err: too many string values for {((RField)p).Prop}");
                }
                else if (pros[nom] is Tex)
                {
                    var f = (RField)p;
                    ((Tex)pros[nom]).Values[pos[nom]] = new TextLan(f.Value, f.Lang);
                    pos[nom]++;
                }
                else if (pros[nom] is Dir)
                {
                    string id1 = ((RLink)p).Resource;
                    RRecord? r1 = getRecord(id1);
                    var shablon1 = ((Dir)shablon.Props[nom]).Resources
                        .FirstOrDefault(res => res.Tp == r1?.Tp);
                    Rec r11 = ToRec(r1, shablon1);
                    ((Dir)pros[nom]).Resources[pos[nom]] = r11;
                    pos[nom]++;
                }
                else if (pros[nom] is Inv)
                {
                    string id1 = ((RInverseLink)p).Source;
                    RRecord? r1 = getRecord(id1);
                    var shablon1 = ((Inv)shablon.Props[nom]).Sources
                        .FirstOrDefault(res => res.Tp == r1?.Tp);
                    Rec r11 = ToRec(r1, shablon1);
                    ((Inv)pros[nom]).Sources[pos[nom]] = r11;
                    pos[nom]++;
                }
            }
            // Добавляем pros, устранив нулевые
            result.Props = pros.Where(p => p != null).ToArray();
            return result;
        }
    }

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
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder($"r({Id}, {Tp}");
            foreach (var p in Props) { sb.Append(", "); sb.Append(p.ToString()); }
            sb.Append(')');
            return sb.ToString();
        }
    }
    public abstract class Pro
    {
        public string Pred { get; internal set; } = "";
    }
    public class Tex : Pro
    {
        public TextLan[] Values { get; internal set; }
        public Tex(string pred, params TextLan[] values)
        {
            this.Pred = pred;
            this.Values = values;
        }
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("t(");
            bool firsttime = true;
            foreach (var v in Values)
            {
                if (!firsttime) sb.Append(", ");
                firsttime = false;
                sb.Append($"\"{v.Text}\"");
                if (v.Lang != null) sb.Append("^^" + v.Lang);
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    public class Str : Pro
    {
        public string? Value { get; internal set; }
        public Str(string pred)
        {
            this.Pred = pred;
        }
        public Str(string pred, string? value)
        {
            this.Pred = pred;
            this.Value = value;
        }
        public override string ToString()
        {
            return $"\"{Value}\"";
        }
    }
    public class Dir : Pro
    {
        public Rec[] Resources { get; internal set; }
        public Dir(string pred, params Rec[] resources) 
        {
            Pred = pred;
            Resources = resources;
        }
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("d(");
            bool firsttime = true;
            foreach (var res in Resources)
            {
                if (!firsttime) sb.Append(", ");
                firsttime = false;
                sb.Append(res.ToString());
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    public class Inv : Pro
    {
        public Rec[] Sources { get; internal set; }
        public Inv(string pred, params Rec[] sources)
        {
            Pred = pred;
            Sources = sources;
        }
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("i(");
            bool firsttime = true;
            foreach (var res in Sources)
            {
                if (!firsttime) sb.Append(", ");
                firsttime = false;
                sb.Append(res.ToString());
            }
            sb.Append(')');
            return sb.ToString();
        }
    }

}
