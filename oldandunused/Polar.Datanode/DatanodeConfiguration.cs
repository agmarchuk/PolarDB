using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Datanode
{
    public partial class Datanode
    {
        // ========================= Вторая часть методов: работа с конфигуратором, вычисление дополнительных структур

        // Поляровский тип конфигуратора
        private PType tp_config = new PTypeRecord(
            new NamedType("Tables", new PTypeSequence(new PTypeRecord(
                new NamedType("tab_name", new PType(PTypeEnumeration.sstring)),
                new NamedType("el_type", PType.TType),
                new NamedType("excolumns", new PTypeSequence(new PType(PTypeEnumeration.integer)))))),
            new NamedType("Nodes", new PTypeSequence(new PTypeRecord(
                new NamedType("", new PType(PTypeEnumeration.sstring)),
                new NamedType("", new PType(PTypeEnumeration.sstring)),
                new NamedType("", new PType(PTypeEnumeration.boolean)),
                new NamedType("", new PType(PTypeEnumeration.sstring)),
                new NamedType("", new PType(PTypeEnumeration.sstring))))),
            new NamedType("Sections", new PTypeSequence(new PTypeRecord(
                new NamedType("tab_ind", new PType(PTypeEnumeration.integer)),
                new NamedType("part_ind", new PType(PTypeEnumeration.integer)),
                new NamedType("node_ind", new PType(PTypeEnumeration.integer))))));
        // Объект конфигуратор. После изменения, надо пересчитывать базу данных
        private object[] configuration = null;

        private PType tp_sections = new PTypeSequence(new PTypeRecord(
                new NamedType("tab_ind", new PType(PTypeEnumeration.integer)),
                new NamedType("part_ind", new PType(PTypeEnumeration.integer)),
                new NamedType("node_ind", new PType(PTypeEnumeration.integer))));
        private void SaveConfiguration(object config)
        {
            if (!ismaster) configuration = (object[])config;
            //PaCell scell = new PaCell(ConfigObject.tp, path + "configuration.pac", false);
            //scell.Clear();
            //scell.Fill(configuration);
            //scell.Close();
            string p = path + "configuration.bin";
            if (File.Exists(p)) File.Delete(p);
            var fs = File.Create(p);
            ByteFlow.Serialize(new BinaryWriter(fs), configuration, ConfigObject.tp);
            fs.Flush();

            if (ismaster)
            { // Послать приказ другим
                int nnodes = ((object[])configuration[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    indicomm.Order(nd - 1, _saveconfiguration, 0, configuration); // нулевая таблица несущественна
                }

            }
        }
        private void LoadConfiguration()
        {
            //PaCell scell = new PaCell(ConfigObject.tp, path + "configuration.pac", false);
            //object[] conf = (object[])scell.Root.Get();
            //configuration = conf;
            //scell.Close();
            string p = path + "configuration.bin";
            var fs = File.Open(p, FileMode.Open);
            configuration = (object[])ByteFlow.Deserialize(new BinaryReader(fs), ConfigObject.tp);

            if (ismaster)
            { // Послать приказ другим. TODO: надо бы сделать посылку конфигурации, тогда можно будет от мастера "плясать"???
                int nnodes = ((object[])configuration[1]).Count();
                for (int nd = 1; nd < nnodes; nd++)
                {
                    indicomm.Order(nd - 1, _loadconfiguration, 0, null); // нулевая таблица несущественна
                }

            }
        }


        private void PrintConfiguration()
        {
            var conf_rec = (object[])configuration;
            object[] tables = (object[])conf_rec[0];
            object[] nodes = (object[])conf_rec[1];
            object[] sections = (object[])conf_rec[2];
            Console.WriteLine("Tables:");
            foreach (object[] tab in tables)
            {
                Console.WriteLine(tab[0]);
                Console.WriteLine(PType.TType.Interpret(((PType)tab[1]).ToPObject(1)));
                Console.WriteLine(new PTypeSequence(new PType(PTypeEnumeration.integer)).Interpret(tab[2]));
            }
            Console.WriteLine("Nodes:");
            Console.WriteLine("Sections:");
        }

        private List<KVSequencePortion> portions;
        struct Secrecord { public int table, section, portion; }
        private List<Secrecord> sections;
        //private Func<int, int, int> tabsec2por;

        public void InitFunctions()
        {
            int ntabs = ((object[])configuration[0]).Length;

            var allparts = ((object[])(configuration[2]))
                .Cast<object[]>()
                //.Where(sec => (int)sec[2] == 0)
                .ToArray();
            // ключ к таблице превращается в слой. Таблица и слой определяют часть, которая должна быть активирована.
            // Точнее, определяется узел. Индекс части определяется "внутри" узла через sections. 
            tablay2node = (tb, ly) =>
            {
                var qu = allparts.First(prt => (int)prt[0] == tb && (int)prt[1] == ly);
                return (int)qu[2];
            };
            // вычисление количества порций в таблице
            tab2n = tb =>
            {
                var qu = allparts.Count(prt => (int)prt[0] == tb);
                return qu;
            };
            tab2t = tb =>
            {
                return element_types[tb];
            };
            tab2indxs = tb =>
            {
                return ((object[])((object[])((object[])configuration[0])[tb])[2]).Cast<int>().ToArray();
            };
        }
    }
}
