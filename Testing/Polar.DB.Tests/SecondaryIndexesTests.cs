using System.IO;
using Polar.Universal;
using Xunit;

namespace Polar.DB.Tests;

public class SecondaryIndexesTests
{
    private static readonly PTypeRecord RecordType = new(
        new NamedType("id", new PType(PTypeEnumeration.integer)),
        new NamedType("name", new PType(PTypeEnumeration.sstring)),
        new NamedType("age", new PType(PTypeEnumeration.integer)),
        new NamedType("tags", new PTypeSequence(new PType(PTypeEnumeration.sstring))));

    [Fact]
    public void SVector_UVector_UVec_And_UIndex_Return_Expected_Results_AfterBuild()
    {
        using var scope = new IndexedSequenceScope();

        var sIndex = new SVectorIndex(scope.StreamGen, scope.Sequence, r => new[] { (string)((object[])r)[1] });
        var ageIndex = new UVectorIndex(scope.StreamGen, scope.Sequence, new PType(PTypeEnumeration.integer), r => new IComparable[] { (int)((object[])r)[2] });
        var tagIndex = new UVecIndex(scope.StreamGen, scope.Sequence, TagsOf, tag => Hashfunctions.HashRot13((string)tag), ignorecase: true);
        var exactNameIndex = new UIndex(
            scope.StreamGen,
            scope.Sequence,
            applicable: _ => true,
            hashFunc: r => Hashfunctions.HashRot13((string)((object[])r)[1]),
            comp: Comparer<object>.Create((a, b) => string.Compare((string)((object[])a)[1], (string)((object[])b)[1], StringComparison.Ordinal)));

        scope.Sequence.uindexes = new IUIndex[] { sIndex, ageIndex, tagIndex, exactNameIndex };

        scope.Sequence.Load(new object[]
        {
            new object[] { 1, "ALICE", 30, new object[] { "news", "tech" } },
            new object[] { 2, "BOB", 40, new object[] { "sports" } },
            new object[] { 3, "ANNA", 30, new object[] { "news" } }
        });
        scope.Sequence.Build();

        var byName = scope.Sequence.GetAllByValue(0, "alice", _ => Array.Empty<IComparable>()).Cast<object[]>().ToArray();
        Assert.Single(byName);
        Assert.Equal(1, (int)byName[0][0]);

        var byLike = scope.Sequence.GetAllByLike(0, "AL").Cast<object[]>().ToArray();
        Assert.Single(byLike);
        Assert.Equal(1, (int)byLike[0][0]);

        var byAge = scope.Sequence.GetAllByValue(1, 30, _ => Array.Empty<IComparable>()).Cast<object[]>().ToArray();
        Assert.Equal(2, byAge.Length);
        Assert.Equal(new[] { 1, 3 }, byAge.Select(r => (int)r[0]).OrderBy(x => x).ToArray());

        var byTag = scope.Sequence.GetAllByValue(2, "NEWS", TagsOf, ignorecase: true).Cast<object[]>().ToArray();
        Assert.Equal(2, byTag.Length);
        Assert.Equal(new[] { 1, 3 }, byTag.Select(r => (int)r[0]).OrderBy(x => x).ToArray());

        var sample = new object[] { 0, "BOB", 0, Array.Empty<object>() };
        var bySample = scope.Sequence.GetAllBySample(3, sample).Cast<object[]>().ToArray();
        Assert.Single(bySample);
        Assert.Equal(2, (int)bySample[0][0]);
    }

    [Fact]
    public void SVector_UVector_And_UVec_See_Dynamic_Appends_Without_Rebuild()
    {
        using var scope = new IndexedSequenceScope();

        var sIndex = new SVectorIndex(scope.StreamGen, scope.Sequence, r => new[] { (string)((object[])r)[1] });
        var ageIndex = new UVectorIndex(scope.StreamGen, scope.Sequence, new PType(PTypeEnumeration.integer), r => new IComparable[] { (int)((object[])r)[2] });
        var tagIndex = new UVecIndex(scope.StreamGen, scope.Sequence, TagsOf, tag => Hashfunctions.HashRot13((string)tag), ignorecase: true);

        scope.Sequence.uindexes = new IUIndex[] { sIndex, ageIndex, tagIndex };

        scope.Sequence.Load(new object[]
        {
            new object[] { 1, "ALICE", 30, new object[] { "news" } }
        });
        scope.Sequence.Build();

        scope.Sequence.AppendElement(new object[] { 2, "BOB", 35, new object[] { "sport", "news" } });

        var byName = scope.Sequence.GetAllByValue(0, "bob", _ => Array.Empty<IComparable>()).Cast<object[]>().ToArray();
        Assert.Single(byName);
        Assert.Equal(2, (int)byName[0][0]);

        var byAge = scope.Sequence.GetAllByValue(1, 35, _ => Array.Empty<IComparable>()).Cast<object[]>().ToArray();
        Assert.Single(byAge);
        Assert.Equal(2, (int)byAge[0][0]);

        var byTag = scope.Sequence.GetAllByValue(2, "NEWS", TagsOf, ignorecase: true).Cast<object[]>().ToArray();
        Assert.Equal(2, byTag.Length);
        Assert.Equal(new[] { 1, 2 }, byTag.Select(r => (int)r[0]).OrderBy(x => x).ToArray());
    }

    private static IEnumerable<IComparable> TagsOf(object record)
    {
        return ((object[])((object[])record)[3]).Cast<IComparable>();
    }

    private sealed class IndexedSequenceScope : IDisposable
    {
        private readonly string _tempDir;
        private int _fileNo;

        public IndexedSequenceScope()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "PolarDbTests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_tempDir);

            Sequence = new USequence(
                RecordType,
                Path.Combine(_tempDir, "state.bin"),
                StreamGen,
                _ => false,
                value => (int)((object[])value)[0],
                key => (int)key,
                optimise: false);
        }

        public USequence Sequence { get; }

        public Stream StreamGen()
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
