using System.IO;
using System.Reflection;
using Polar.Universal;
using Xunit;

namespace Polar.DB.Tests;

public class USequenceTests
{
    private static readonly PTypeRecord PersonType = new(
        new NamedType("id", new PType(PTypeEnumeration.integer)),
        new NamedType("name", new PType(PTypeEnumeration.sstring)));

    [Fact]
    public void Load_Skips_Empty_Records_And_Writes_State_File()
    {
        using var scope = new USequenceScope(PersonType, isEmpty: r => string.IsNullOrEmpty((string)((object[])r)[1]));

        scope.Sequence.Load(new object[]
        {
            new object[] { 1, "Alice" },
            new object[] { 2, "" },
            new object[] { 3, "Bob" }
        });

        var values = scope.Sequence.ElementValues().Cast<object[]>().ToArray();
        Assert.Equal(2, values.Length);
        Assert.Equal(1, (int)values[0][0]);
        Assert.Equal(3, (int)values[1][0]);

        using var reader = new BinaryReader(File.OpenRead(scope.StateFilePath));
        Assert.Equal(2L, reader.ReadInt64());
        Assert.True(reader.ReadInt64() > 8L);
    }

    [Fact]
    public void Build_Writes_State_And_GetByKey_Returns_Value()
    {
        using var scope = new USequenceScope(PersonType);

        scope.Sequence.AppendElement(new object[] { 10, "Alice" });
        scope.Sequence.AppendElement(new object[] { 20, "Bob" });
        scope.Sequence.Build();

        var found = Assert.IsType<object[]>(scope.Sequence.GetByKey(20));
        Assert.Equal("Bob", Assert.IsType<string>(found[1]));

        using var reader = new BinaryReader(File.OpenRead(scope.StateFilePath));
        Assert.Equal(2L, reader.ReadInt64());
        Assert.True(reader.ReadInt64() > 8L);
    }

    [Fact]
    public void ElementValues_And_Scan_Use_Only_Latest_Duplicate_Key()
    {
        using var scope = new USequenceScope(PersonType);

        scope.Sequence.AppendElement(new object[] { 1, "Old" });
        scope.Sequence.AppendElement(new object[] { 1, "New" });
        scope.Sequence.AppendElement(new object[] { 2, "Bob" });

        var values = scope.Sequence.ElementValues()
            .Cast<object[]>()
            .Select(r => ((int)r[0], (string)r[1]))
            .ToArray();

        Assert.Equal(new[] { (1, "New"), (2, "Bob") }, values);

        var scanned = new List<(long Off, int Id, string Name)>();
        scope.Sequence.Scan((off, obj) =>
        {
            var record = (object[])obj;
            scanned.Add((off, (int)record[0], (string)record[1]));
            return true;
        });

        Assert.Equal(2, scanned.Count);
        Assert.Contains(scanned, x => x.Id == 1 && x.Name == "New");
        Assert.Contains(scanned, x => x.Id == 2 && x.Name == "Bob");
    }

    [Fact]
    public void CorrectOnAppendElement_Indexes_Record_Added_Directly_To_Base_Sequence()
    {
        using var scope = new USequenceScope(PersonType);

        var baseSequence = scope.GetBaseSequence();
        baseSequence.Clear();
        long offset = baseSequence.AppendElement(new object[] { 77, "Manual" });
        baseSequence.Flush();

        scope.Sequence.CorrectOnAppendElement(offset);

        var found = Assert.IsType<object[]>(scope.Sequence.GetByKey(77));
        Assert.Equal("Manual", Assert.IsType<string>(found[1]));
    }

    [Fact]
    public void RestoreDynamic_Indexes_Records_Appended_After_Last_Saved_State()
    {
        using var scope = new USequenceScope(PersonType);

        scope.Sequence.Load(new object[]
        {
            new object[] { 1, "Alice" }
        });
        scope.Sequence.Build();

        var baseSequence = scope.GetBaseSequence();
        baseSequence.AppendElement(new object[] { 2, "Bob" });
        baseSequence.Flush();

        scope.Sequence.RestoreDynamic();

        var found = Assert.IsType<object[]>(scope.Sequence.GetByKey(2));
        Assert.Equal("Bob", Assert.IsType<string>(found[1]));
    }

    private sealed class USequenceScope : IDisposable
    {
        private readonly string _tempDir;
        private int _fileNo;

        public USequenceScope(PType type, Func<object, bool>? isEmpty = null)
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "PolarDbTests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_tempDir);
            StateFilePath = Path.Combine(_tempDir, "state.bin");

            Sequence = new USequence(
                type,
                StateFilePath,
                StreamGen,
                isEmpty ?? (_ => false),
                value => (int)((object[])value)[0],
                key => (int)key,
                optimise: false);
        }

        public USequence Sequence { get; }
        public string StateFilePath { get; }

        public UniversalSequenceBase GetBaseSequence()
        {
            var field = typeof(USequence).GetField("sequence", BindingFlags.Instance | BindingFlags.NonPublic)!;
            return (UniversalSequenceBase)field.GetValue(Sequence)!;
        }

        private Stream StreamGen()
        {
            return new FileStream(
                Path.Combine(_tempDir, $"f{_fileNo++}.bin"),
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite);
        }

        public void Dispose()
        {
            try { Sequence.Close(); } catch { }
            try
            {
                if (Directory.Exists(_tempDir))
                    Directory.Delete(_tempDir, recursive: true);
            }
            catch { }
        }
    }
}
