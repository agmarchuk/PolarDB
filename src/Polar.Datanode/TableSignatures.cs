using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using PolarDB;
using Polar.DB;

namespace Polar.Datanode
{
    class TableSignatures
    {
        private PType tp_record;
        public Tuple<PType, PType>[] signatures;
        public TableSignatures(PType tp_rec)
        {
            // table_ind, part_ind, node_ind, streams: [integer] (? набор номеров стримов для данной части)
            PType tp_sections = new PTypeSequence(new PTypeRecord(
                new NamedType("table_ind", new PType(PTypeEnumeration.integer)),
                new NamedType("section_ind", new PType(PTypeEnumeration.integer)),
                new NamedType("node_ind", new PType(PTypeEnumeration.integer))));
            tp_record = tp_rec;
            signatures = new Tuple<PType, PType>[] {
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // пустая команда для тестирования
                new Tuple<PType, PType>(new PType(PTypeEnumeration.integer),
                    new PTypeRecord(
                        new NamedType("id", new PType(PTypeEnumeration.integer)),
                        new NamedType("nm", new PType(PTypeEnumeration.sstring)),
                        new NamedType("ag", new PType(PTypeEnumeration.integer)))), // sendinttest - имитация Get(k)
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // Clear()
                new Tuple<PType, PType>(new PType(PTypeEnumeration.integer), new PType(PTypeEnumeration.none)), // Init(nodenum)
                new Tuple<PType, PType>(new PTypeRecord(
                    new NamedType("pair",  tp_record),
                    new NamedType("dynindex", new PType(PTypeEnumeration.boolean))),
                    new PType(PTypeEnumeration.none)), // AppendOnlyRecord(tab, record, bool dynindex)
                new Tuple<PType, PType>(new PTypeRecord(
                    new NamedType("indx_nom", new PType(PTypeEnumeration.integer)),
                    new NamedType("ext_key", new PType(PTypeEnumeration.integer)),
                    new NamedType("pri_key", new PType(PTypeEnumeration.integer)),
                    new NamedType("dynindex", new PType(PTypeEnumeration.boolean))),
                    new PType(PTypeEnumeration.none)), // AppendOnlyExtKey(int tab, int indx_nom, int ext_key, int pri_key, bool dynindex)
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // Flush()
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // CalculateStaticIndex()
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // Activate()
                new Tuple<PType, PType>(new PTypeRecord(
                    //new NamedType("tab", new PType(PTypeEnumeration.integer)),
                    new NamedType("key", new PType(PTypeEnumeration.integer))),
                    tp_record), // GetByKey(tab, key) -> tp_record
                new Tuple<PType, PType>(new PTypeRecord(
                    //new NamedType("tab", new PType(PTypeEnumeration.integer)),
                    new NamedType("exindnom", new PType(PTypeEnumeration.integer)),
                    new NamedType("exkey", new PType(PTypeEnumeration.integer))),
                    new PTypeSequence(new PType(PTypeEnumeration.integer))), // GetAllPrimaryByExternal(tab, exindnom, exkey) -> object[] { prikey,... }
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // CreateDatabase()
                new Tuple<PType, PType>(ConfigObject.tp, new PType(PTypeEnumeration.none)), // SaveConfiguration(configuration)
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // LoadConfiguration()
                new Tuple<PType, PType>(new PType(PTypeEnumeration.none), new PType(PTypeEnumeration.none)), // ActivateDatabase()
                new Tuple<PType, PType>(ConfigObject.tp, new PType(PTypeEnumeration.none)), // SetConfiguration(conf)
            }.ToArray();
        }
    }
}
