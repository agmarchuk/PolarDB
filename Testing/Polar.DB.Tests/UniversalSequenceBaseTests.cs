using System.IO;
using Xunit;

namespace Polar.DB.Tests;

public class UniversalSequenceBaseTests
{
    private static UniversalSequenceBase CreateFixedLongSequence(MemoryStream stream)
    {
        return new UniversalSequenceBase(new PType(PTypeEnumeration.longinteger), stream);
    }

    [Fact]
    public void Clear_ResetsState_AndPlacesCursorAtAppendOffset()
    {
        var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.AppendElement(10L);
        sequence.AppendElement(20L);
        sequence.Flush();

        stream.Position = 0L;
        sequence.Clear();

        Assert.Equal(0L, sequence.Count());
        Assert.Equal(8L, sequence.AppendOffset);
        Assert.Equal(8L, stream.Position);
        Assert.Equal(8L, stream.Length);
        Assert.Equal(0L, BitConverter.ToInt64(stream.ToArray(), 0));
    }

    [Fact]
    public void AppendElement_UsesLogicalTail_UpdatesState_AndRestoresPosition()
    {
        var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();

        stream.Position = 0L;
        long firstOffset = sequence.AppendElement(11L);

        Assert.Equal(8L, firstOffset);
        Assert.Equal(1L, sequence.Count());
        Assert.Equal(16L, sequence.AppendOffset);
        Assert.Equal(0L, stream.Position);

        stream.Position = 5L;
        long secondOffset = sequence.AppendElement(22L);

        Assert.Equal(16L, secondOffset);
        Assert.Equal(2L, sequence.Count());
        Assert.Equal(24L, sequence.AppendOffset);
        Assert.Equal(5L, stream.Position);
    }

    [Fact]
    public void Flush_WritesHeader_AndRestoresPosition()
    {
        var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.AppendElement(20L);

        stream.Position = 3L;
        sequence.Flush();

        Assert.Equal(3L, stream.Position);
        Assert.Equal(2L, BitConverter.ToInt64(stream.ToArray(), 0));
    }

    [Fact]
    public void GetElement_ByOffset_ReturnsValue_AndRestoresPosition()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(11L);
        sequence.AppendElement(22L);
        sequence.Flush();

        stream.Position = 3L;
        long value = (long)sequence.GetElement(16L);

        Assert.Equal(22L, value);
        Assert.Equal(3L, stream.Position);
    }

    [Fact]
    public void GetTypedElement_ByOffset_ReturnsValue_AndRestoresPosition()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(123L);
        sequence.Flush();

        stream.Position = 2L;
        long value = (long)sequence.GetTypedElement(new PType(PTypeEnumeration.longinteger), 8L);

        Assert.Equal(123L, value);
        Assert.Equal(2L, stream.Position);
    }

    [Fact]
    public void SetElement_ByOffset_RewritesExistingValue_AndRestoresPosition()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(11L);
        sequence.AppendElement(22L);
        sequence.Flush();

        stream.Position = 1L;
        sequence.SetElement(99L, 16L);

        Assert.Equal(1L, stream.Position);
        Assert.Equal(2L, sequence.Count());
        Assert.Equal(24L, sequence.AppendOffset);
        Assert.Equal(99L, (long)sequence.GetByIndex(1));
    }

    [Fact]
    public void ElementValues_RestoresPosition_AndReturnsAllValues()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(11L);
        sequence.AppendElement(22L);
        sequence.AppendElement(33L);
        sequence.Flush();

        stream.Position = 2L;
        long[] values = sequence.ElementValues().Select(v => (long)v).ToArray();

        Assert.Equal(new[] { 11L, 22L, 33L }, values);
        Assert.Equal(2L, stream.Position);
    }

    [Fact]
    public void ElementValues_Range_RestoresPosition_AndReturnsRequestedValues()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(11L);
        sequence.AppendElement(22L);
        sequence.AppendElement(33L);
        sequence.Flush();

        stream.Position = 4L;
        long[] values = sequence.ElementValues(16L, 2L).Select(v => (long)v).ToArray();

        Assert.Equal(new[] { 22L, 33L }, values);
        Assert.Equal(4L, stream.Position);
    }

    [Fact]
    public void Scan_RestoresPosition_AndSupportsEarlyStop()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(11L);
        sequence.AppendElement(22L);
        sequence.AppendElement(33L);
        sequence.Flush();

        var readValues = new List<long>();
        var readOffsets = new List<long>();

        stream.Position = 7L;
        sequence.Scan((off, element) =>
        {
            readOffsets.Add(off);
            readValues.Add((long)element);
            return readValues.Count < 2;
        });

        Assert.Equal(new[] { 8L, 16L }, readOffsets);
        Assert.Equal(new[] { 11L, 22L }, readValues);
        Assert.Equal(7L, stream.Position);
    }

    [Fact]
    public void ElementOffsetValuePairs_RestoresPosition_AndReturnsOffsetsAndValues()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(11L);
        sequence.AppendElement(22L);
        sequence.AppendElement(33L);
        sequence.Flush();

        stream.Position = 6L;
        var pairs = sequence.ElementOffsetValuePairs().ToArray();

        Assert.Equal(3, pairs.Length);
        Assert.Equal(8L, pairs[0].Item1);
        Assert.Equal(11L, (long)pairs[0].Item2);
        Assert.Equal(16L, pairs[1].Item1);
        Assert.Equal(22L, (long)pairs[1].Item2);
        Assert.Equal(24L, pairs[2].Item1);
        Assert.Equal(33L, (long)pairs[2].Item2);
        Assert.Equal(6L, stream.Position);
    }

    [Fact]
    public void Refresh_RecalculatesVariableSizeTail_AndMovesCursorToAppendOffset()
    {
        var stream = new MemoryStream();
        var sequence = CreateVariableSequence(stream);

        sequence.Clear();
        sequence.AppendElement(new object[] { 1, "A" });
        sequence.AppendElement(new object[] { 2, "BB" });
        sequence.Flush();

        long expectedAppendOffset = sequence.AppendOffset;

        stream.Position = 0L;
        sequence.Refresh();

        Assert.Equal(2L, sequence.Count());
        Assert.Equal(expectedAppendOffset, sequence.AppendOffset);
        Assert.Equal(expectedAppendOffset, stream.Position);
    }

    [Fact]
    public void Refresh_TrimsGarbageTail_ForVariableSizeSequence()
    {
        var stream = new MemoryStream();
        var personType = CreateVariableSequenceType();
        var sequence = new UniversalSequenceBase(personType, stream);

        sequence.Clear();
        sequence.AppendElement(new object[] { 1, "A" });
        sequence.AppendElement(new object[] { 2, "BB" });
        sequence.Flush();

        long expectedAppendOffset = sequence.AppendOffset;
        long expectedCount = sequence.Count();

        stream.Position = stream.Length;
        using (var tailWriter = new BinaryWriter(stream, System.Text.Encoding.Default, true))
        {
            ByteFlow.Serialize(tailWriter, new object[] { 3, "CCC" }, personType);
        }

        stream.Position = 0L;
        sequence.Refresh();

        Assert.Equal(expectedCount, sequence.Count());
        Assert.Equal(expectedAppendOffset, sequence.AppendOffset);
        Assert.Equal(expectedAppendOffset, stream.Length);
        Assert.Equal(expectedAppendOffset, stream.Position);
    }

    [Fact]
    public void GetByIndex_ForVariableSizeSequence_ThrowsInvalidOperationException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateVariableSequence(stream);

        sequence.Clear();
        sequence.AppendElement(new object[] { 1, "A" });
        sequence.Flush();

        Assert.Throws<InvalidOperationException>(() => sequence.GetByIndex(0));
    }

    [Fact]
    public void ElementOffset_ForVariableSizeSequence_ThrowsInvalidOperationException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateVariableSequence(stream);

        sequence.Clear();
        sequence.AppendElement(new object[] { 1, "A" });
        sequence.Flush();

        Assert.Throws<InvalidOperationException>(() => sequence.ElementOffset(0));
    }

    [Fact]
    public void Sort32_SortsFixedSizeSequence_InAscendingOrder()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(30L);
        sequence.AppendElement(10L);
        sequence.AppendElement(20L);
        sequence.Flush();

        sequence.Sort32(v => checked((int)(long)v));

        Assert.Equal(3L, sequence.Count());
        Assert.Equal(8L + 3L * sizeof(long), sequence.AppendOffset);
        Assert.Equal(10L, (long)sequence.GetByIndex(0));
        Assert.Equal(20L, (long)sequence.GetByIndex(1));
        Assert.Equal(30L, (long)sequence.GetByIndex(2));
    }

    [Fact]
    public void Sort64_SortsFixedSizeSequence_InAscendingOrder()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(300L);
        sequence.AppendElement(100L);
        sequence.AppendElement(200L);
        sequence.Flush();

        sequence.Sort64(v => (long)v);

        Assert.Equal(3L, sequence.Count());
        Assert.Equal(8L + 3L * sizeof(long), sequence.AppendOffset);
        Assert.Equal(100L, (long)sequence.GetByIndex(0));
        Assert.Equal(200L, (long)sequence.GetByIndex(1));
        Assert.Equal(300L, (long)sequence.GetByIndex(2));
    }

    private static PType CreateVariableSequenceType()
    {
        return new PTypeRecord(
            new NamedType("id", new PType(PTypeEnumeration.integer)),
            new NamedType("name", new PType(PTypeEnumeration.sstring)));
    }

    private static UniversalSequenceBase CreateVariableSequence(MemoryStream stream)
    {
        return new UniversalSequenceBase(CreateVariableSequenceType(), stream);
    }
    
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


    [Fact]
    public void ElementOffset_WithoutArgument_ReturnsAppendOffset()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        Assert.Equal(sequence.AppendOffset, sequence.ElementOffset());

        sequence.AppendElement(10L);
        Assert.Equal(sequence.AppendOffset, sequence.ElementOffset());
    }

    [Fact]
    public void GetElement_WhenOffsetIsBeforeHeader_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.GetElement(7L));
    }

    [Fact]
    public void GetElement_WhenOffsetEqualsAppendOffset_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.GetElement(sequence.AppendOffset));
    }

    [Fact]
    public void GetTypedElement_WhenTypeIsNull_ThrowsArgumentNullException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentNullException>(() => sequence.GetTypedElement(null!, 8L));
    }

    [Fact]
    public void SetElement_WhenOffsetIsGreaterThanAppendOffset_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.SetElement(20L, sequence.AppendOffset + 1L));
    }

    [Fact]
    public void SetTypedElement_WhenTypeIsNull_ThrowsArgumentNullException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentNullException>(() => sequence.SetTypedElement(null!, 20L, 8L));
    }

    [Fact]
    public void GetByIndex_WhenIndexIsNegative_ThrowsIndexOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<IndexOutOfRangeException>(() => sequence.GetByIndex(-1));
    }

    [Fact]
    public void GetByIndex_WhenIndexEqualsCount_ThrowsIndexOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<IndexOutOfRangeException>(() => sequence.GetByIndex(1));
    }

    [Fact]
    public void ElementOffset_WhenIndexIsNegative_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.ElementOffset(-1));
    }

    [Fact]
    public void ElementOffset_WhenIndexEqualsCount_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.ElementOffset(1));
    }

    [Fact]
    public void ElementValues_Range_WhenOffsetIsBeforeHeader_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.ElementValues(7L, 1L).ToArray());
    }

    [Fact]
    public void ElementValues_Range_WhenNumberIsNegative_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.ElementValues(8L, -1L).ToArray());
    }

    [Fact]
    public void ElementOffsetValuePairs_Range_WhenOffsetIsBeforeHeader_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.ElementOffsetValuePairs(7L, 1L).ToArray());
    }

    [Fact]
    public void ElementOffsetValuePairs_Range_WhenNumberIsNegative_ThrowsArgumentOutOfRangeException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentOutOfRangeException>(() => sequence.ElementOffsetValuePairs(8L, -1L).ToArray());
    }

    [Fact]
    public void Scan_WhenHandlerIsNull_ThrowsArgumentNullException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentNullException>(() => sequence.Scan(null!));
    }

    [Fact]
    public void Constructor_RecalculatesCountAndAppendOffset_ForFixedSizeStream()
    {
        using var stream = new MemoryStream();

        var writerSequence = CreateFixedLongSequence(stream);
        writerSequence.Clear();
        writerSequence.AppendElement(10L);
        writerSequence.AppendElement(20L);
        writerSequence.AppendElement(30L);
        writerSequence.Flush();

        stream.Position = 1L;

        var reopenedSequence = CreateFixedLongSequence(stream);

        Assert.Equal(3L, reopenedSequence.Count());
        Assert.Equal(32L, reopenedSequence.AppendOffset);
        Assert.Equal(32L, stream.Position);
        Assert.Equal(20L, (long)reopenedSequence.GetByIndex(1));
    }

    [Fact]
    public void Recovery_DoesNotCountGarbageTail_ForFixedSizeSequence()
    {
        using var stream = new MemoryStream();
        var writer = new BinaryWriter(stream);

        writer.Write(2L);
        writer.Write(10);
        writer.Write(20);
        writer.Write(30);
        writer.Flush();

        stream.Position = 0L;

        var sequence = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream);

        long expectedLength = 8L + 2 * sizeof(int);

        Assert.Equal(2L, sequence.Count());
        Assert.Equal(expectedLength, sequence.AppendOffset);
        Assert.Equal(expectedLength, stream.Length);
        Assert.Equal(2L, BitConverter.ToInt64(stream.ToArray(), 0));
    }

    [Fact]
    public void Recovery_TrimsTail_ForVariableSizeSequence()
    {
        using var stream = new MemoryStream();
        var personType = CreateVariableSequenceType();
        var sequence = new UniversalSequenceBase(personType, stream);

        sequence.Clear();
        sequence.AppendElement(new object[] { 1, "A" });
        sequence.AppendElement(new object[] { 2, "BB" });
        sequence.Flush();

        long expectedLength = sequence.AppendOffset;

        stream.Position = stream.Length;
        using (var tailWriter = new BinaryWriter(stream, System.Text.Encoding.Default, true))
        {
            ByteFlow.Serialize(tailWriter, new object[] { 3, "CCC" }, personType);
        }

        stream.Position = 0L;
        var reopened = new UniversalSequenceBase(personType, stream);

        Assert.Equal(2L, reopened.Count());
        Assert.Equal(expectedLength, reopened.AppendOffset);
        Assert.Equal(expectedLength, stream.Length);
        Assert.Equal(2L, BitConverter.ToInt64(stream.ToArray(), 0));
    }

    [Fact]
    public void Recovery_TruncatesOverdeclaredCount_ForFixedSizeStream()
    {
        using var stream = new MemoryStream();
        var writer = new BinaryWriter(stream);

        writer.Write(3L);
        writer.Write(100);
        writer.Write(200);
        writer.Flush();

        stream.Position = 0L;
        var sequence = new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream);

        Assert.Equal(2L, sequence.Count());
        Assert.Equal(8L + 2 * sizeof(int), sequence.AppendOffset);
        Assert.Equal(2L, BitConverter.ToInt64(stream.ToArray(), 0));
        Assert.Equal(200, (int)sequence.GetByIndex(1));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(7)]
    public void Constructor_ThrowsOnPartialHeader(int headerLength)
    {
        var stream = new MemoryStream(new byte[headerLength]);

        Assert.Throws<InvalidDataException>(
            () => new UniversalSequenceBase(new PType(PTypeEnumeration.integer), stream));

        Assert.Equal(headerLength, stream.Length);
    }

    [Fact]
    public void Refresh_WhenPhysicalStreamBecomesEmpty_ReinitializesEmptySequence()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.AppendElement(20L);
        sequence.Flush();

        stream.SetLength(0L);
        stream.Position = 0L;

        sequence.Refresh();

        Assert.Equal(0L, sequence.Count());
        Assert.Equal(8L, sequence.AppendOffset);
        Assert.Equal(8L, stream.Position);
        Assert.Equal(8L, stream.Length);
        Assert.Equal(0L, BitConverter.ToInt64(stream.ToArray(), 0));
    }

    [Fact]
    public void AppendElement_AfterElementOffsetValuePairsEnumeration_AppendsAtLogicalTail()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.AppendElement(20L);
        sequence.Flush();

        stream.Position = 5L;
        _ = sequence.ElementOffsetValuePairs().ToArray();

        long offset = sequence.AppendElement(30L);

        Assert.Equal(24L, offset);
        Assert.Equal(32L, sequence.AppendOffset);
        Assert.Equal(3L, sequence.Count());
        Assert.Equal(5L, stream.Position);
        Assert.Equal(30L, (long)sequence.GetByIndex(2));
    }

    [Fact]
    public void SetTypedElement_RewritesExistingValue_AndRestoresPosition()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.AppendElement(20L);
        sequence.Flush();

        stream.Position = 6L;
        sequence.SetTypedElement(new PType(PTypeEnumeration.longinteger), 77L, 8L);

        Assert.Equal(6L, stream.Position);
        Assert.Equal(2L, sequence.Count());
        Assert.Equal(24L, sequence.AppendOffset);
        Assert.Equal(77L, (long)sequence.GetByIndex(0));
        Assert.Equal(20L, (long)sequence.GetByIndex(1));
    }

    [Fact]
    public void Sort32_WhenKeySelectorIsNull_ThrowsArgumentNullException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentNullException>(() => sequence.Sort32(null!));
    }

    [Fact]
    public void Sort64_WhenKeySelectorIsNull_ThrowsArgumentNullException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(10L);
        sequence.Flush();

        Assert.Throws<ArgumentNullException>(() => sequence.Sort64(null!));
    }

    [Fact]
    public void Sort32_WhenSequenceHasVariableSizeElements_ThrowsInvalidOperationException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateVariableSequence(stream);

        sequence.Clear();
        sequence.AppendElement(new object[] { 2, "B" });
        sequence.AppendElement(new object[] { 1, "A" });
        sequence.Flush();

        Assert.Throws<InvalidOperationException>(() => sequence.Sort32(_ => 0));
    }

    [Fact]
    public void Sort64_WhenSequenceHasVariableSizeElements_ThrowsInvalidOperationException()
    {
        using var stream = new MemoryStream();
        var sequence = CreateVariableSequence(stream);

        sequence.Clear();
        sequence.AppendElement(new object[] { 2, "B" });
        sequence.AppendElement(new object[] { 1, "A" });
        sequence.Flush();

        Assert.Throws<InvalidOperationException>(() => sequence.Sort64(_ => 0L));
    }

    [Fact]
    public void Sort32_WhenSingleElementExists_DoesNotChangeSequence()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(42L);
        sequence.Flush();

        long appendOffsetBefore = sequence.AppendOffset;

        sequence.Sort32(v => checked((int)(long)v));

        Assert.Equal(1L, sequence.Count());
        Assert.Equal(appendOffsetBefore, sequence.AppendOffset);
        Assert.Equal(42L, (long)sequence.GetByIndex(0));
    }

    [Fact]
    public void Sort64_WhenSingleElementExists_DoesNotChangeSequence()
    {
        using var stream = new MemoryStream();
        var sequence = CreateFixedLongSequence(stream);

        sequence.Clear();
        sequence.AppendElement(42L);
        sequence.Flush();

        long appendOffsetBefore = sequence.AppendOffset;

        sequence.Sort64(v => (long)v);

        Assert.Equal(1L, sequence.Count());
        Assert.Equal(appendOffsetBefore, sequence.AppendOffset);
        Assert.Equal(42L, (long)sequence.GetByIndex(0));
    }
}
