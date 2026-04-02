namespace Polar.DB
{
    /// <summary>
    /// Helper for working with record values represented as object[] according to a PTypeRecord schema.
    /// Centralizes field name to index mapping and shape validation.
    /// </summary>
    public sealed class RecordAccessor
    {
        private readonly PTypeRecord _recordType;
        private readonly Dictionary<string, int> _fieldIndexes;

        public RecordAccessor(PTypeRecord recordType)
        {
            _recordType = recordType ?? throw new ArgumentNullException(nameof(recordType));
            _fieldIndexes = recordType.Fields
                .Select((field, index) => new { field.Name, Index = index })
                .ToDictionary(x => x.Name, x => x.Index, StringComparer.Ordinal);
        }

        public PTypeRecord RecordType => _recordType;
        public int FieldCount => _recordType.Fields.Length;
        public IEnumerable<string> FieldNames => _recordType.Fields.Select(f => f.Name);

        public bool HasField(string fieldName)
        {
            if (fieldName == null) throw new ArgumentNullException(nameof(fieldName));
            return _fieldIndexes.ContainsKey(fieldName);
        }

        public int GetIndex(string fieldName)
        {
            if (fieldName == null) throw new ArgumentNullException(nameof(fieldName));
            if (!_fieldIndexes.TryGetValue(fieldName, out int index))
                throw new ArgumentException($"Unknown field '{fieldName}'.", nameof(fieldName));

            return index;
        }

        public PType GetFieldType(string fieldName)
        {
            return _recordType.Fields[GetIndex(fieldName)].Type;
        }

        public object[] CreateRecord()
        {
            return new object[FieldCount];
        }

        public object[] CreateRecord(params object[] values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (values.Length != FieldCount)
                throw new ArgumentException(
                    $"Record field count mismatch. Expected {FieldCount}, got {values.Length}.",
                    nameof(values));

            return values;
        }

        public void ValidateShape(object record)
        {
            if (record == null) throw new ArgumentNullException(nameof(record));
            if (record is not object[] arr)
                throw new ArgumentException("Record value must be object[].", nameof(record));
            if (arr.Length != FieldCount)
                throw new ArgumentException(
                    $"Record field count mismatch. Expected {FieldCount}, got {arr.Length}.",
                    nameof(record));
        }

        public object Get(object record, string fieldName)
        {
            ValidateShape(record);
            return ((object[])record)[GetIndex(fieldName)];
        }

        public T Get<T>(object record, string fieldName)
        {
            return (T)Get(record, fieldName);
        }

        public void Set(object record, string fieldName, object value)
        {
            ValidateShape(record);
            ((object[])record)[GetIndex(fieldName)] = value;
        }

        public bool TryGet(object record, string fieldName, out object value)
        {
            value = null;
            if (record is not object[] arr) return false;
            if (!_fieldIndexes.TryGetValue(fieldName, out int index)) return false;
            if (arr.Length != FieldCount) return false;

            value = arr[index];
            return true;
        }

        public bool TryGet<T>(object record, string fieldName, out T value)
        {
            value = default(T);
            if (!TryGet(record, fieldName, out object raw)) return false;
            if (raw is T typed)
            {
                value = typed;
                return true;
            }

            return false;
        }
    }
}
