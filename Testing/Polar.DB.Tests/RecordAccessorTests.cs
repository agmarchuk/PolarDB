using Xunit;

namespace Polar.DB.Tests;

public class RecordAccessorTests
{
    private static readonly PTypeRecord PersonType = new(
        new NamedType("id", new PType(PTypeEnumeration.integer)),
        new NamedType("name", new PType(PTypeEnumeration.sstring)),
        new NamedType("age", new PType(PTypeEnumeration.integer)));

    [Fact]
    public void GetIndex_Returns_Stable_Field_Position()
    {
        var accessor = new RecordAccessor(PersonType);

        Assert.Equal(0, accessor.GetIndex("id"));
        Assert.Equal(1, accessor.GetIndex("name"));
        Assert.Equal(2, accessor.GetIndex("age"));
    }

    [Fact]
    public void Get_And_Set_By_Field_Name_Work()
    {
        var accessor = new RecordAccessor(PersonType);
        object record = new object[] { 7, "Ivanov", 20 };

        Assert.Equal(20, accessor.Get<int>(record, "age"));

        accessor.Set(record, "age", 21);

        Assert.Equal(21, accessor.Get<int>(record, "age"));
    }

    [Fact]
    public void CreateRecord_Creates_Array_With_Expected_Field_Count()
    {
        var accessor = new RecordAccessor(PersonType);

        var record = accessor.CreateRecord(1, "Petrov", 33);

        Assert.Equal(3, record.Length);
        Assert.Equal(1, record[0]);
        Assert.Equal("Petrov", record[1]);
        Assert.Equal(33, record[2]);
    }

    [Fact]
    public void ValidateShape_Throws_On_Invalid_Field_Count()
    {
        var accessor = new RecordAccessor(PersonType);
        object invalid = new object[] { 1, "OnlyTwoFields" };

        var ex = Assert.Throws<ArgumentException>(() => accessor.ValidateShape(invalid));
        Assert.Contains("Record field count mismatch", ex.Message);
    }

    [Fact]
    public void TryGet_Returns_False_For_Missing_Field()
    {
        var accessor = new RecordAccessor(PersonType);
        object record = new object[] { 7, "Ivanov", 20 };

        var ok = accessor.TryGet(record, "missing", out var value);

        Assert.False(ok);
        Assert.Null(value);
    }
}
