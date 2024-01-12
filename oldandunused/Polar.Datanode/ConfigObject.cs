using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Polar.DB;

namespace Polar.Datanode
{
    class ConfigObject
    {
        public static PType tp = new PTypeRecord(
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
        public ConfigObject()
        {
        }
        private object[] configuration_value;
        public object[] Value { get { return configuration_value; } }
        public int Ntabs { get { return ((object[])configuration_value[0]).Length; } }

        public void DropTables(int nnodes)
        {
            // Дефолтное значение конфигуратора: нет таблиц, nnode узлов (мастер и слэйвы), нет секций
            configuration_value = new object[]
            {
                new object[0],
                Enumerable.Range(0, nnodes).Select(i => new object[] { "", "", i==0, "", "" }).ToArray(),
                new object[0]
            };
        }
        public int Nnodes { get { return ((object[])configuration_value[1]).Length; } }
        public int CreateTable(string name, PType tp_el, int[] exkey_columns)
        {
            int nom = ((object[])configuration_value[0]).Length;
            configuration_value[0] = ((object[])configuration_value[0]).Concat(new object[] {
                new object[] { name, tp_el.ToPObject(99), exkey_columns.Select(i => (object)i).ToArray() }
            }).ToArray();
            return nom;
        }
        public void CreateSections(string tabname, int[] innodes)
        {
            int tab = ((object[])configuration_value[0])
                .Select((ob, i) => new Tuple<int, object>(i, ob))
                .First(tu => (string)((object[])tu.Item2)[0] == tabname).Item1;
            if (innodes.Length != ((object[])configuration_value[1]).Length) throw new Exception("Err: 28181");
            int section = 0;
            for (int i = 0; i < innodes.Length; i++)
            {
                for (int j = 0; j < innodes[i]; j++)
                {
                    configuration_value[2] = ((object[])configuration_value[2]).Concat(new object[]
                    {
                        new object[] { tab, section, i }
                    }).ToArray();
                    section++;
                }
            }
        }
        public void FillSectionConf()
        {
            // Примитивный алгоритм: каждая таблица имеет по одной секции на каждом узле
            configuration_value[2] = Enumerable.Range(0, Ntabs)
                .SelectMany(t => Enumerable.Range(0, Nnodes)
                .Select(n => new object[] { t, n, n }).ToArray()).ToArray();
        }
    }
}
