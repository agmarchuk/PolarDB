namespace Polar.CellIndexes
{
    public struct TableRow
    {
        public object Row;
        public long Offset;

        public TableRow(object row, long offset)
        {
            Row = row;
            Offset = offset;
        }
    }
}
