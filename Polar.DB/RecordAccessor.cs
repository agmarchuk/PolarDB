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

        /// <summary>
        /// Создает новый аксессор для указанной схемы записи.
        /// </summary>
        /// <param name="recordType">Схема записи, определяющая имена полей, порядок и типы.</param>
        /// <exception cref="ArgumentNullException">Выбрасывается, если <paramref name="recordType"/> равен <see langword="null"/>.</exception>
        public RecordAccessor(PTypeRecord recordType)
        {
            _recordType = recordType ?? throw new ArgumentNullException(nameof(recordType));
            _fieldIndexes = recordType.Fields
                .Select((field, index) => new { field.Name, Index = index })
                .ToDictionary(x => x.Name, x => x.Index, StringComparer.Ordinal);
        }

        /// <summary>
        /// Возвращает схему записи, используемую аксессором.
        /// </summary>
        public PTypeRecord RecordType => _recordType;
        /// <summary>
        /// Возвращает количество полей, определённых в схеме.
        /// </summary>
        public int FieldCount => _recordType.Fields.Length;
        /// <summary>
        /// Перечисляет все имена полей в порядке схемы.
        /// </summary>
        public IEnumerable<string> FieldNames => _recordType.Fields.Select(f => f.Name);

        /// <summary>
        /// Определяет, содержит ли схема поле с заданным именем.
        /// </summary>
        /// <param name="fieldName">Имя поля для проверки.</param>
        /// <returns><see langword="true"/>, если поле существует; иначе <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException">Выбрасывается при <see langword="null"/> в <paramref name="fieldName"/>.</exception>
        public bool HasField(string fieldName)
        {
            if (fieldName == null) throw new ArgumentNullException(nameof(fieldName));
            return _fieldIndexes.ContainsKey(fieldName);
        }

        /// <summary>
        /// Возвращает нулевой индекс поля в массиве записи.
        /// </summary>
        /// <param name="fieldName">Имя поля.</param>
        /// <returns>Нулевой индекс поля.</returns>
        /// <exception cref="ArgumentNullException">Выбрасывается при <see langword="null"/> в <paramref name="fieldName"/>.</exception>
        /// <exception cref="ArgumentException">Выбрасывается, если имя поля отсутствует в схеме.</exception>
        public int GetIndex(string fieldName)
        {
            if (fieldName == null) throw new ArgumentNullException(nameof(fieldName));
            if (!_fieldIndexes.TryGetValue(fieldName, out int index))
                throw new ArgumentException($"Unknown field '{fieldName}'.", nameof(fieldName));

            return index;
        }

        /// <summary>
        /// Возвращает тип поля по имени согласно схеме.
        /// </summary>
        /// <param name="fieldName">Имя поля.</param>
        /// <returns>Тип поля, объявленный в схеме.</returns>
        public PType GetFieldType(string fieldName)
        {
            return _recordType.Fields[GetIndex(fieldName)].Type;
        }

        /// <summary>
        /// Создает пустой массив записи с нужным числом полей.
        /// </summary>
        /// <returns>Новый экземпляр записи как массив <see cref="object"/>.</returns>
        public object[] CreateRecord()
        {
            return new object[FieldCount];
        }

        /// <summary>
        /// Создает запись из переданных значений после проверки количества полей.
        /// </summary>
        /// <param name="values">Значения полей в порядке схемы.</param>
        /// <returns>Тот же массив <paramref name="values"/>, если проверка прошла.</returns>
        /// <exception cref="ArgumentNullException">Выбрасывается при <see langword="null"/> в <paramref name="values"/>.</exception>
        /// <exception cref="ArgumentException">Выбрасывается, если число значений не соответствует схеме.</exception>
        public object[] CreateRecord(params object[] values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (values.Length != FieldCount)
                throw new ArgumentException(
                    $"Record field count mismatch. Expected {FieldCount}, got {values.Length}.",
                    nameof(values));

            return values;
        }

        /// <summary>
        /// Проверяет, является ли объект массивом записи с формой, соответствующей схеме.
        /// </summary>
        /// <param name="record">Объект, ожидаемый как массив <see cref="object"/> нужной длины.</param>
        /// <exception cref="ArgumentNullException">Выбрасывается, если <paramref name="record"/> равен <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">
        /// Выбрасывается, если <paramref name="record"/> не является массивом <see cref="object"/>
        /// или его длина не совпадает с количеством полей схемы.
        /// </exception>
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

        /// <summary>
        /// Получает значение поля по имени из записи.
        /// </summary>
        /// <param name="record">Экземпляр записи как массив <see cref="object"/>.</param>
        /// <param name="fieldName">Имя поля для чтения.</param>
        /// <returns>Значение поля, хранящееся в записи.</returns>
        public object Get(object record, string fieldName)
        {
            ValidateShape(record);
            return ((object[])record)[GetIndex(fieldName)];
        }

        /// <summary>
        /// Получает значение поля по имени и приводит его к требуемому типу.
        /// </summary>
        /// <typeparam name="T">Ожидаемый тип значения поля.</typeparam>
        /// <param name="record">Экземпляр записи как массив <see cref="object"/>.</param>
        /// <param name="fieldName">Имя поля для чтения.</param>
        /// <returns>Значение поля, приведенное к <typeparamref name="T"/>.</returns>
        public T Get<T>(object record, string fieldName)
        {
            return (T)Get(record, fieldName);
        }

        /// <summary>
        /// Записывает значение поля по имени в записи.
        /// </summary>
        /// <param name="record">Экземпляр записи как массив <see cref="object"/>.</param>
        /// <param name="fieldName">Имя поля для записи.</param>
        /// <param name="value">Новое значение поля.</param>
        public void Set(object record, string fieldName, object value)
        {
            ValidateShape(record);
            ((object[])record)[GetIndex(fieldName)] = value;
        }

        /// <summary>
        /// Пытается получить значение поля без исключений при некорректной форме записи или неизвестном поле.
        /// </summary>
        /// <param name="record">Экземпляр записи как массив <see cref="object"/>.</param>
        /// <param name="fieldName">Имя поля для чтения.</param>
        /// <param name="value">
        /// После возврата содержит значение поля при успехе; иначе <see langword="null"/>.
        /// </param>
        /// <returns><see langword="true"/>, если значение считано успешно; иначе <see langword="false"/>.</returns>
        public bool TryGet(object record, string fieldName, out object value)
        {
            value = null;
            if (record is not object[] arr) return false;
            if (!_fieldIndexes.TryGetValue(fieldName, out int index)) return false;
            if (arr.Length != FieldCount) return false;

            value = arr[index];
            return true;
        }

        /// <summary>
        /// Пытается получить значение поля и привести его к указанному типу.
        /// </summary>
        /// <typeparam name="T">Ожидаемый тип значения поля.</typeparam>
        /// <param name="record">Экземпляр записи как массив <see cref="object"/>.</param>
        /// <param name="fieldName">Имя поля для чтения.</param>
        /// <param name="value">
        /// После возврата содержит приведенное значение при успехе;
        /// иначе значение по умолчанию для <typeparamref name="T"/>.
        /// </param>
        /// <returns>
        /// <see langword="true"/> при успешном чтении и совместимости с <typeparamref name="T"/>;
        /// иначе <see langword="false"/>.
        /// </returns>
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
