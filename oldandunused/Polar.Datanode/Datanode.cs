using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.Datanode
{
    public partial class Datanode
    {
        private int nodecode = 0; // У мастера он не переопределяется, у слэйвов - переопределяется
        private bool ismaster = false;
        public bool Ismaster { get { return ismaster; } }
        private string masterhost = "unknown";
        private int indiport = 6001;

        private string path = @"D:\Home\data\datanode1\";
        private string myhost = "0.0.0.0";

        private TableSignatures[] tass; // по одной на таблицу

        public Datanode()
        {
            // Техническое значение на одну условную таблицу с типом элементов запись одного целого, это требуется для общих методов посылки приказов 
            tass = new TableSignatures[] {
                new TableSignatures(new PTypeRecord(new NamedType("", new PType(PTypeEnumeration.integer)))) };
        }

        // ======================== Первая часть методов - инициирование коммуникаций, тестирования коммуникаций =====================

        // Техническая функция: добавляет слеш в конец path'а, если его нет
        private static Func<string, string> CorrectPath = pth => { char l = pth[pth.Length - 1]; return l != '/' && l != '\\' ? pth = pth + "/" : pth; };

        private Communal bosscomm = null;
        private Individual indicomm = null;
        private IndividualSpeed speed_test = null;
        // Публичные методы
        public void Master(string path)
        {
            path = CorrectPath(path);
            this.path = path;
            nodecode = 0;
            ismaster = true;
            bosscomm = new Communal(myhost, 5007, true); // Адреса быть не должно...
            indicomm = new Individual(true, masterhost, indiport, tass, null); // мастеру не положено пользоваться функцией
            speed_test = new IndividualSpeed(true, masterhost, 6002);
        }
        public void Slave(string path, string masterhost)
        {
            path = CorrectPath(path);
            this.path = path;
            this.masterhost = masterhost;
            ismaster = false;
            bosscomm = new Communal(masterhost, 5007, false);
            indicomm = new Individual(false, masterhost, indiport, tass, ExecComm);
            speed_test = new IndividualSpeed(false, masterhost, 6002);
        }
        public string Ask()
        {
            var asker = bosscomm.AskBoss("from client");
            var qu = Task<string>.Run(() => asker);
            return qu.Result;
        }
        public object Order(int chan, int command, object mess)
        {
            object v = indicomm.Order(chan, command, 0, mess); // Берем нулевую таблицу
            return v;
        }
        public void TestOrder() { speed_test.TestOrder(); }
    }
}
