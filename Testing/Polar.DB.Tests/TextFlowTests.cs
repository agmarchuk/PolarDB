using System.IO;
using Xunit;

namespace Polar.DB.Tests;

public class TextFlowTests
{
    [Fact]
    public void Serialize_And_Deserialize_Record_RoundTrip()
    {
        var type = new PTypeRecord(
            new NamedType("id", new PType(PTypeEnumeration.integer)),
            new NamedType("name", new PType(PTypeEnumeration.sstring)));

        using var writer = new StringWriter();
        TextFlow.Serialize(writer, new object[] { 5, "Petrov" }, type);

        using var reader = new StringReader(writer.ToString());
        var restored = Assert.IsType<object[]>(TextFlow.Deserialize(reader, type));

        Assert.Equal(5, Assert.IsType<int>(restored[0]));
        Assert.Equal("Petrov", Assert.IsType<string>(restored[1]));
    }

    [Fact]
    public void Serialize_Escapes_String_Content()
    {
        using var writer = new StringWriter();
        TextFlow.Serialize(writer, "a\\b\"c", new PType(PTypeEnumeration.sstring));

        Assert.Equal("\"a\\\\b\\\"c\"", writer.ToString());
    }

    [Fact]
    public void Deserialize_String_Parses_Escape_Sequences()
    {
        using var reader = new StringReader("\"line1\\nline2\\t\\\\quote:\\\"\"");
        var restored = Assert.IsType<string>(TextFlow.Deserialize(reader, new PType(PTypeEnumeration.sstring)));

        Assert.Equal("line1\nline2\t\\quote:\"", restored);
    }

    [Fact]
    public void SerializeFormatted_ForNestedRecord_AddsLineBreaks()
    {
        var nestedType = new PTypeRecord(
            new NamedType("id", new PType(PTypeEnumeration.integer)),
            new NamedType(
                "payload",
                new PTypeRecord(
                    new NamedType("name", new PType(PTypeEnumeration.sstring)),
                    new NamedType("age", new PType(PTypeEnumeration.integer)))));

        using var writer = new StringWriter();
        TextFlow.SerializeFormatted(writer, new object[] { 1, new object[] { "Ann", 25 } }, nestedType, 0);

        var text = writer.ToString();
        Assert.Contains('\n', text);
        Assert.Contains("Ann", text);
        Assert.Contains("25", text);
    }

    [Fact]
    public void SerializeFlowToSequense_And_DeserializeSequenseToFlow_RoundTrip()
    {
        using var writer = new StringWriter();
        TextFlow.SerializeFlowToSequense(writer, new object[] { 10, 20, 30 }, new PType(PTypeEnumeration.integer));

        using var reader = new StringReader(writer.ToString());
        var restored = TextFlow.DeserializeSequenseToFlow(reader, new PType(PTypeEnumeration.integer))
            .Cast<int>()
            .ToArray();

        Assert.Equal(new[] { 10, 20, 30 }, restored);
    }

    [Fact]
    public void Serialize_And_Deserialize_Union_RoundTrip()
    {
        var type = new PTypeUnion(
            new NamedType("i", new PType(PTypeEnumeration.integer)),
            new NamedType("s", new PType(PTypeEnumeration.sstring)));

        using var writer = new StringWriter();
        TextFlow.Serialize(writer, new object[] { 1, "abc" }, type);

        using var reader = new StringReader(writer.ToString());
        var restored = Assert.IsType<object[]>(TextFlow.Deserialize(reader, type));

        Assert.Equal(1, Assert.IsType<int>(restored[0]));
        Assert.Equal("abc", Assert.IsType<string>(restored[1]));
    }
}
