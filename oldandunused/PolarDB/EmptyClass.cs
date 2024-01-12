using System;
using Polar.DB;
using Polar.Cells;
using Polar.CellIndexes;
using Polar.PagedStreams;


namespace PolarDB
{
    public class EmptyClass
    {
        EmptyClass()
        {
            PType tp = new PType(PTypeEnumeration.integer);
            Polar.Cells.PaCell cell = null;
            Polar.CellIndexes.TableView table;
            Polar.PagedStreams.FileOfBlocks fob;
        }

    }
}
