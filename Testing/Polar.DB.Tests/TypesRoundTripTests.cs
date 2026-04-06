using Xunit;

namespace Polar.DB.Tests;

public class TypesRoundTripTests
{
    [Fact]
    public void FString_RoundTrip_Preserves_Length()
    {
        var original = new PTypeFString(16);

        var po = original.ToPObject(8);
        var restored = PType.FromPObject(po);

        var fstring = Assert.IsType<PTypeFString>(restored);
        Assert.Equal(16, fstring.Length);
        Assert.Equal(original.HeadSize, restored.HeadSize);
    }

    [Fact]
    public void Sequence_RoundTrip_Preserves_Growing_And_ElementType()
    {
        var original = new PTypeSequence(new PType(PTypeEnumeration.integer), true);

        var po = original.ToPObject(8);
        var restored = PType.FromPObject(po);

        var sequence = Assert.IsType<PTypeSequence>(restored);
        Assert.True(sequence.Growing);
        Assert.Equal(PTypeEnumeration.integer, sequence.ElementType.Vid);
    }

    [Fact]
    public void Record_RoundTrip_Preserves_Field_Order_And_Field_Types()
    {
        var original = new PTypeRecord(
            new NamedType("id", new PType(PTypeEnumeration.integer)),
            new NamedType("name", new PType(PTypeEnumeration.sstring)),
            new NamedType("code", new PTypeFString(12)));

        var po = original.ToPObject(8);
        var restored = Assert.IsType<PTypeRecord>(PType.FromPObject(po));

        Assert.Equal(3, restored.Fields.Length);
        Assert.Equal("id", restored.Fields[0].Name);
        Assert.Equal(PTypeEnumeration.integer, restored.Fields[0].Type.Vid);
        Assert.Equal("name", restored.Fields[1].Name);
        Assert.Equal(PTypeEnumeration.sstring, restored.Fields[1].Type.Vid);
        Assert.Equal("code", restored.Fields[2].Name);

        var codeType = Assert.IsType<PTypeFString>(restored.Fields[2].Type);
        Assert.Equal(12, codeType.Length);
    }

    [Fact]
    public void Union_RoundTrip_Preserves_Variants()
    {
        var original = new PTypeUnion(
            new NamedType("i", new PType(PTypeEnumeration.integer)),
            new NamedType("s", new PType(PTypeEnumeration.sstring)));

        var po = original.ToPObject(8);
        var restored = Assert.IsType<PTypeUnion>(PType.FromPObject(po));

        Assert.Equal(2, restored.Variants.Length);
        Assert.Equal("i", restored.Variants[0].Name);
        Assert.Equal(PTypeEnumeration.integer, restored.Variants[0].Type.Vid);
        Assert.Equal("s", restored.Variants[1].Name);
        Assert.Equal(PTypeEnumeration.sstring, restored.Variants[1].Type.Vid);
    }

    [Fact]
    public void ToPObject_With_Negative_Level_Returns_Null()
    {
        var original = new PTypeRecord(
            new NamedType("id", new PType(PTypeEnumeration.integer)));

        var po = original.ToPObject(-1);
        Assert.Null(po);
    }
}