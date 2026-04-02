using Xunit;

namespace Polar.DB.Tests;

public class UniversalSequenceBaseTests
{
    [Fact]
    public void Append_And_GetByIndex_Work_For_Fixed_Size_Element_Type()
    {
        using var stream = new MemoryStream();
        var sequence = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream);

        sequence.Clear();
        sequence.AppendElement(10);
        sequence.AppendElement(20);
        sequence.AppendElement(30);
        sequence.Flush();

        Assert.Equal(3, sequence.Count());
        Assert.Equal(10, sequence.GetByIndex(0));
        Assert.Equal(20, sequence.GetByIndex(1));
        Assert.Equal(30, sequence.GetByIndex(2));
    }

    [Fact]
    public void ElementOffset_For_Fixed_Size_Type_Is_Computed_By_Index()
    {
        using var stream = new MemoryStream();
        var sequence = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream);

        sequence.Clear();
        sequence.AppendElement(100);
        sequence.AppendElement(200);
        sequence.Flush();

        Assert.Equal(8L, sequence.ElementOffset(0));
        Assert.Equal(12L, sequence.ElementOffset(1));
        Assert.Equal(16L, sequence.AppendOffset);
    }

    [Fact]
    public void Scan_Visits_All_Elements_In_Order()
    {
        using var stream = new MemoryStream();
        var sequence = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream);

        sequence.Clear();
        sequence.AppendElement(1);
        sequence.AppendElement(2);
        sequence.AppendElement(3);
        sequence.Flush();

        var items = new List<int>();
        sequence.Scan((off, obj) =>
        {
            items.Add((int)obj);
            return true;
        });

        Assert.Equal(new[] { 1, 2, 3 }, items);
    }

    [Fact]
    public void Refresh_Recalculates_AppendOffset_For_Fixed_Size_Sequence()
    {
        using var stream = new MemoryStream();
        var writer = new BinaryWriter(stream);

        writer.Write(2L);
        writer.Write(100);
        writer.Write(200);
        writer.Flush();

        stream.Position = 0;

        var sequence = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream);

        Assert.Equal(2, sequence.Count());
        Assert.Equal(16L, sequence.AppendOffset);
        Assert.Equal(100, sequence.GetByIndex(0));
        Assert.Equal(200, sequence.GetByIndex(1));
    }
}