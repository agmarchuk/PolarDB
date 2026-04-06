//using Polar.DB;

namespace Polar.DB;

/// <summary>
///     Класс предоставляет последовательность элементов типа tp_elem, опирающуюся на индекс или полуиндекс ключей типа
///     Tkey
///     Представляет последовательность элементов, хранящуюся в бинарном потоке:
///     в начале потока расположен заголовок с количеством элементов,
///     далее — сериализованные элементы указанного полярного типа.
/// </summary>
/// <remarks>
///     Класс поддерживает добавление элементов в логический конец последовательности,
///     чтение по смещению, последовательный обход и операции обслуживания,
///     такие как обновление внутреннего состояния, очистка и сортировка.
///     В нормализованном состоянии логический конец последовательности хранится в <see cref="AppendOffset" />
///     и совпадает с физической длиной потока <see cref="Stream.Length" />.
/// </remarks>
public class UniversalSequenceBase
{
    /// <summary>
    ///     Описывает полярный тип элемента, используемый для сериализации и десериализации.
    /// </summary>
    /// <remarks>
    ///     Определяет формат хранения каждого элемента последовательности
    ///     и используется методами чтения и записи по умолчанию.
    /// </remarks>
    protected PType tp_elem; // Поляровский тип элемента

    /// <summary>
    ///     Содержит поток-носитель последовательности.
    /// </summary>
    /// <remarks>
    ///     В первых 8 байтах потока хранится количество элементов,
    ///     далее подряд размещаются сериализованные элементы.
    ///     Текущее значение <see cref="Stream.Position" /> является рабочим курсором потока,
    ///     но не считается источником истины для логического конца последовательности.
    /// </remarks>
    protected Stream
        fs; // Стрим - среда для последовательности. Сначала 8 байтов длина, потом подряд бинарные развертки элементов

    /// <summary>
    ///     Возвращает поток-носитель последовательности.
    /// </summary>
    /// <value>
    ///     Поток, в котором расположены заголовок и сериализованные элементы.
    /// </value>
    /// <remarks>
    ///     Свойство предназначено для внутреннего использования.
    ///     При прямой работе с потоком важно не нарушать инварианты
    ///     <see cref="AppendOffset" /> и количества элементов.
    /// </remarks>
    internal Stream Media => fs;

    /// <summary>
    ///     Выполняет бинарное чтение из потока последовательности.
    /// </summary>
    /// <remarks>
    ///     Используется внутренними low-level методами чтения.
    ///     Читает данные из текущей позиции <see cref="fs" />.
    /// </remarks>
    private readonly BinaryReader br;

    /// <summary>
    ///     Выполняет бинарную запись в поток последовательности.
    /// </summary>
    /// <remarks>
    ///     Используется внутренними low-level методами записи.
    ///     Пишет данные в текущую позицию <see cref="fs" />.
    /// </remarks>
    private readonly BinaryWriter bw;

    /// <summary>
    ///     Хранит размер одного элемента в байтах, если элементы имеют фиксированную длину.
    /// </summary>
    /// <remarks>
    ///     Для последовательностей переменного размера содержит значение <c>-1</c>.
    ///     Используется для быстрого вычисления смещений элементов и логического конца последовательности.
    /// </remarks>
    protected int elem_size = -1; // длина элемента, если она фиксирована, иначе -1

    /// <summary>
    ///     Хранит текущее логическое количество элементов в последовательности.
    /// </summary>
    /// <remarks>
    ///     В нормализованном состоянии совпадает со значением,
    ///     записанным в заголовке потока.
    /// </remarks>
    private long nelements; // текущее количество элеметов. В "покое" - совпадает со значением в первых 8 байтах

    /// <summary>
    ///     Инициализирует последовательность на указанном потоке
    ///     и подготавливает внутреннее состояние для работы с элементами заданного типа.
    /// </summary>
    /// <param name="tp_el">
    ///     Описание полярного типа элемента, используемое для сериализации и десериализации.
    /// </param>
    /// <param name="media">
    ///     Поток-носитель, в котором размещаются заголовок последовательности и её элементы.
    /// </param>
    /// <remarks>
    ///     Конструктор определяет режим фиксированного или переменного размера элемента,
    ///     создаёт бинарные reader/writer и выполняет одноразовый recovery:
    ///     находит последний валидный элемент, отрезает мусорный хвост,
    ///     пересчитывает количество элементов и синхронизирует заголовок.
    ///     После этого устанавливается инвариант:
    ///     <see cref="AppendOffset" /> == <see cref="Stream.Length" />.
    /// </remarks>
    public UniversalSequenceBase(PType tp_el, Stream media)
    {
        tp_elem = tp_el ?? throw new ArgumentNullException(nameof(tp_el));
        if (tp_elem.HasNoTail) elem_size = tp_elem.HeadSize;

        fs = media ?? throw new ArgumentNullException(nameof(media));
        br = new BinaryReader(fs);
        bw = new BinaryWriter(fs);

        RecoverAndNormalizeState();
    }

    /// <summary>
    ///     Выполняет стартовый recovery по физическому содержимому потока.
    /// </summary>
    /// <remarks>
    ///     Метод один раз восстанавливает фактический логический конец последовательности,
    ///     отрезает мусорный хвост, пересчитывает количество валидных элементов
    ///     и переписывает заголовок с корректным количеством элементов.
    ///     Значение declared element count в заголовке рассматривается как намерение о числе элементов,
    ///     но фактически читаемое количество может оказаться меньше, если данные обрезаны.
    ///     После нормализации логический конец последовательности, хранящийся в
    ///     <see cref="AppendOffset" />, совпадает с физической длиной потока,
    ///     что гарантирует консистентность для всех последующих записей.
    /// </remarks>
    private void RecoverAndNormalizeState()
    {
        if (fs.Length == 0L)
        {
            Clear();
            return;
        }

        if (fs.Length < 8L)
        {
            // Ни один валидный заголовок не может занимать меньше восьми байт,
            // поэтому мы не трогаем поток и явно сигнализируем о повреждении.
            throw new InvalidDataException("Sequence stream is corrupted: header is shorter than 8 bytes.");
        }

        fs.Position = 0L;
        long declaredCount = br.ReadInt64();

        if (declaredCount < 0L)
            throw new InvalidDataException("Sequence stream is corrupted: negative element count.");

        long recoveredCount = declaredCount;
        long recoveredAppendOffset = 8L;

        if (elem_size > 0)
        {
            long expectedLength;

            try
            {
                expectedLength = checked(8L + declaredCount * elem_size);
            }
            catch (OverflowException)
            {
                throw new InvalidDataException(
                    "Sequence stream is corrupted: declared element count is too large.");
            }

            long actualLength = fs.Length;

            if (expectedLength <= actualLength)
            {
                recoveredAppendOffset = expectedLength;
                if (actualLength != expectedLength) fs.SetLength(expectedLength);
                // Дополнительные полноценные записи после declared count считаются мусорным хвостом.
            }
            else
            {
                long availablePayload = Math.Max(0L, actualLength - 8L);
                long recoveredFullElements = availablePayload / elem_size;
                recoveredCount = recoveredFullElements;
                recoveredAppendOffset = checked(8L + recoveredFullElements * elem_size);
                if (fs.Length != recoveredAppendOffset) fs.SetLength(recoveredAppendOffset);
            }
        }
        else
        {
            fs.Position = 8L;
            recoveredAppendOffset = 8L;
            recoveredCount = 0L;

            for (long i = 0; i < declaredCount; i++)
            {
                long recordStart = fs.Position;

                try
                {
                    ByteFlow.Deserialize(br, tp_elem);
                    recoveredAppendOffset = fs.Position;
                    recoveredCount++;
                }
                catch (EndOfStreamException)
                {
                    fs.Position = recordStart;
                    break;
                }
                catch (IOException)
                {
                    fs.Position = recordStart;
                    break;
                }
                catch (InvalidDataException)
                {
                    fs.Position = recordStart;
                    break;
                }
            }

            if (fs.Length != recoveredAppendOffset) fs.SetLength(recoveredAppendOffset);
        }

        if (recoveredAppendOffset < 8L || recoveredAppendOffset > fs.Length)
            throw new InvalidDataException("Sequence stream recovery produced an invalid append offset.");

        nelements = recoveredCount;
        AppendOffset = recoveredAppendOffset;

        WriteHeaderElementCount();
        EnsureAppendOffsetInvariant();
        fs.Flush();
        fs.Position = AppendOffset;
    }

    /// <summary>
    ///     Делает последовательность с нулевым количеством элементов.
    /// </summary>
    /// <remarks>
    ///     Метод обрезает поток, записывает пустой заголовок,
    ///     сбрасывает количество элементов в ноль и устанавливает
    ///     <see cref="AppendOffset" /> в позицию после заголовка.
    ///     После завершения поток готов к добавлению новых элементов.
    /// </remarks>
    public void Clear()
    {
        fs.SetLength(0L);
        fs.Position = 0L;
        bw.Write(0L);

        nelements = 0L;
        AppendOffset = fs.Length;

        EnsureAppendOffsetInvariant();
        fs.Flush();
        fs.Position = AppendOffset;
    }

    /// <summary>
    ///     Сохраняет актуальное количество элементов в заголовке потока
    ///     и сбрасывает буферы записи.
    /// </summary>
    /// <remarks>
    ///     Метод обновляет первые 8 байт потока значением текущего количества элементов.
    ///     Рабочая позиция потока после выполнения восстанавливается.
    ///     Метод также проверяет согласованность логического конца последовательности
    ///     с физической длиной потока.
    /// </remarks>
    public void Flush()
    {
        long savedPosition = fs.Position;

        try
        {
            EnsureAppendOffsetInvariant();
            WriteHeaderElementCount();
            fs.Flush();
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Сохраняет текущее состояние последовательности и закрывает поток-носитель.
    /// </summary>
    /// <remarks>
    ///     Перед закрытием вызывает <see cref="Flush()" />,
    ///     чтобы количество элементов в заголовке соответствовало внутреннему состоянию.
    ///     После закрытия дальнейшая работа с экземпляром класса не предполагается.
    /// </remarks>
    public void Close()
    {
        Flush();
        fs.Close();
    }

    /// <summary>
    ///     Обновляет внутреннее состояние последовательности по текущему содержимому потока
    ///     без полного пересканирования элементов.
    /// </summary>
    /// <remarks>
    ///     После стартового recovery класс живёт с инвариантом
    ///     <see cref="AppendOffset" /> == <see cref="Stream.Length" />.
    ///     Поэтому Refresh перестраивает внутреннее состояние по declared count из заголовка.
    ///     При фиксированном размере элементов достаточно сверить физическую длину с ожидаемой,
    ///     а для переменного размера приходится заново пройти ровно declared count элементов,
    ///     поскольку <see cref="Stream.Length" /> может включать мусорный хвост и не отражает
    ///     логический конец последовательности.
    /// </remarks>
    public void Refresh()
    {
        if (fs.Length == 0L)
        {
            Clear();
            return;
        }

        if (fs.Length < 8L)
            throw new InvalidDataException("Sequence stream is corrupted: header is shorter than 8 bytes.");

        fs.Position = 0L;
        nelements = br.ReadInt64();

        if (nelements < 0L)
            throw new InvalidDataException("Sequence stream is corrupted: negative element count.");

        if (elem_size > 0)
        {
            long expectedLength;

            try
            {
                expectedLength = checked(8L + nelements * elem_size);
            }
            catch (OverflowException)
            {
                throw new InvalidDataException(
                    "Sequence stream is corrupted: declared element count is too large.");
            }

            if (expectedLength != fs.Length)
                throw new InvalidDataException(
                    "Sequence stream is corrupted: fixed-size payload length does not match the declared element count.");

            AppendOffset = expectedLength;
            EnsureAppendOffsetInvariant();
            fs.Position = AppendOffset;
            return;
        }

        long declaredCount = nelements;
        long recoveredCount = 0L;
        long recoveredAppendOffset = 8L;
        fs.Position = 8L;

        for (long i = 0; i < declaredCount; i++)
        {
            long recordStart = fs.Position;

            try
            {
                ByteFlow.Deserialize(br, tp_elem);
                recoveredCount++;
                recoveredAppendOffset = fs.Position;
            }
            catch (EndOfStreamException ex)
            {
                fs.Position = recordStart;
                throw new InvalidDataException(
                    "Sequence stream is corrupted: declared element count cannot be read completely.", ex);
            }
            catch (IOException ex)
            {
                fs.Position = recordStart;
                throw new InvalidDataException(
                    "Sequence stream is corrupted: unable to read declared number of elements.", ex);
            }
            catch (InvalidDataException)
            {
                fs.Position = recordStart;
                throw;
            }
        }

        if (recoveredAppendOffset < 8L || recoveredAppendOffset > fs.Length)
            throw new InvalidDataException("Sequence stream recovery produced an invalid append offset.");

        if (fs.Length != recoveredAppendOffset) fs.SetLength(recoveredAppendOffset);

        nelements = recoveredCount;
        AppendOffset = recoveredAppendOffset;

        EnsureAppendOffsetInvariant();
        fs.Flush();
        fs.Position = AppendOffset;
    }

    /// <summary>
    ///     Возвращает текущее логическое количество элементов в последовательности.
    /// </summary>
    /// <returns>
    ///     Количество элементов, которое класс считает находящимся в последовательности.
    /// </returns>
    /// <remarks>
    ///     Метод не читает поток и не зависит от текущей позиции курсора.
    ///     Возвращает внутреннее значение, поддерживаемое классом.
    /// </remarks>
    public long Count()
    {
        return nelements;
    }

    /// <summary>
    ///     Вычисляет смещение элемента по его индексу
    ///     в последовательности с фиксированным размером элемента.
    /// </summary>
    /// <param name="ind">
    ///     Ноль-основанный индекс существующего элемента.
    /// </param>
    /// <returns>
    ///     Смещение начала элемента в потоке.
    /// </returns>
    /// <remarks>
    ///     Метод применим только к последовательностям,
    ///     где размер каждого элемента известен заранее и одинаков для всех записей.
    ///     Для последовательностей переменного размера использовать его нельзя.
    /// </remarks>
    public long ElementOffset(long ind)
    {
        if (elem_size <= 0)
            throw new InvalidOperationException(
                "ElementOffset(index) is available only for fixed-size element sequences.");

        if (ind < 0 || ind >= nelements) throw new ArgumentOutOfRangeException(nameof(ind));

        return checked(8L + ind * elem_size);
    }

    /// <summary>
    ///     Возвращает логический конец последовательности.
    ///     Legacy alias: фактически это logical append offset, а не текущий Stream.Position.
    /// </summary>
    /// <returns>
    ///     Смещение следующей допустимой позиции записи элемента в потоке.
    /// </returns>
    /// <remarks>
    ///     Метод оставлен как совместимый alias.
    ///     По смыслу он возвращает не текущую позицию потока,
    ///     а значение <see cref="AppendOffset" />.
    ///     В новом коде предпочтительнее использовать свойство <see cref="AppendOffset" />.
    /// </remarks>
    public long ElementOffset()
    {
        return AppendOffset;
    }

    /// <summary>
    ///     Возвращает логический конец последовательности.
    /// </summary>
    /// <value>
    ///     Смещение следующей допустимой позиции записи элемента в потоке.
    /// </value>
    /// <remarks>
    ///     Значение отражает не текущую позицию курсора потока,
    ///     а фактический логический конец данных последовательности и считается
    ///     единственным источником истины для всех операций записи.
    ///     После recovery, refresh и append-инструкций инвариант
    ///     <see cref="AppendOffset" /> == <see cref="Stream.Length" /> активно поддерживается,
    ///     но до этого физическая длина потока может быть больше из-за мусора,
    ///     поэтому нельзя полагаться только на <see cref="Stream.Length" />.
    /// </remarks>
    public long AppendOffset { get; private set; } = 8L;

    /// <summary>
    ///     Запись сериализации значения с текущей позиции. Корректна только если либо значение фиксированного размера, либо
    ///     запись ведется в конец
    /// </summary>
    /// <param name="v">
    ///     Значение элемента, которое нужно записать.
    /// </param>
    /// <returns>позиция с которой началась запись</returns>
    /// <remarks>
    ///     Это низкоуровневый primitive записи в текущую позицию.
    ///     Метод не выполняет валидацию смещения и не обновляет
    ///     логический конец последовательности сам по себе.
    ///     Эти обязанности лежат на внешних wrapper-методах.
    /// </remarks>
    public long SetElement(object v)
    {
        long offset = fs.Position;
        ByteFlow.Serialize(bw, v, tp_elem);
        return offset;
    }

    /// <summary>
    ///     Сериализует элемент по указанному смещению в потоке.
    /// </summary>
    /// <param name="v">
    ///     Значение элемента, которое нужно записать.
    /// </param>
    /// <param name="off">
    ///     Смещение начала записи в потоке.
    /// </param>
    /// <remarks>
    ///     Метод является безопасной обёрткой над записью в текущую позицию:
    ///     временно переводит поток к нужному смещению,
    ///     выполняет запись и затем восстанавливает исходную позицию.
    ///     Если запись выполняется в хвост и расширяет поток,
    ///     значение <see cref="AppendOffset" /> обновляется.
    ///     При ошибке хвостовая запись откатывается.
    ///     Защита от записи за <see cref="AppendOffset" /> предотвращает неконтролируемое расширение,
    ///     но сама по себе не делает безопасным in-place изменение переменного элемента,
    ///     потому что сериализованная длина элемента и смещения соседей при этом не пересчитываются.
    /// </remarks>
    public void SetElement(object v, long off)
    {
        if (off < 8L || off > AppendOffset) throw new ArgumentOutOfRangeException(nameof(off));

        long savedPosition = fs.Position;
        long originalLength = fs.Length;
        bool isAppendWrite = off == AppendOffset;

        try
        {
            if (off != fs.Position) fs.Position = off;

            SetElement(v);

            if (isAppendWrite)
            {
                AppendOffset = fs.Length;
                EnsureAppendOffsetInvariant();
            }
            else if (fs.Position > AppendOffset)
            {
                throw new InvalidOperationException(
                    "In-place write has crossed the logical end of the sequence. Use AppendElement for tail growth.");
            }
        }
        catch
        {
            if (isAppendWrite && fs.Length != originalLength)
            {
                fs.SetLength(originalLength);
                AppendOffset = originalLength;
            }

            throw;
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Сериализует значение указанного типа по заданному смещению.
    /// </summary>
    /// <param name="tp">
    ///     Тип, в формате которого должно быть сериализовано значение.
    /// </param>
    /// <param name="v">
    ///     Значение, которое нужно записать.
    /// </param>
    /// <param name="off">
    ///     Смещение начала записи в потоке.
    /// </param>
    /// <remarks>
    ///     Метод полезен, когда требуется записать данные
    ///     не в основном типе последовательности, а в произвольном совместимом формате.
    ///     Исходная позиция потока после выполнения восстанавливается.
    ///     При записи в хвост логический конец последовательности обновляется.
    ///     При ошибке хвостовая запись откатывается.
    ///     Как и в <see cref="SetElement(object,long)" />, защита от выхода за
    ///     <see cref="AppendOffset" /> не учитывает длину сериализации переменного элемента,
    ///     поэтому in-place перезапись всё ещё может нарушить расположение соседних записей.
    /// </remarks>
    public void SetTypedElement(PType tp, object v, long off)
    {
        if (tp == null) throw new ArgumentNullException(nameof(tp));

        if (off < 8L || off > AppendOffset) throw new ArgumentOutOfRangeException(nameof(off));

        long savedPosition = fs.Position;
        long originalLength = fs.Length;
        bool isAppendWrite = off == AppendOffset;

        try
        {
            if (off != fs.Position) fs.Position = off;

            ByteFlow.Serialize(bw, v, tp);

            if (isAppendWrite)
            {
                AppendOffset = fs.Length;
                EnsureAppendOffsetInvariant();
            }
            else if (fs.Position > AppendOffset)
            {
                throw new InvalidOperationException(
                    "In-place write has crossed the logical end of the sequence. Use AppendElement for tail growth.");
            }
        }
        catch
        {
            if (isAppendWrite && fs.Length != originalLength)
            {
                fs.SetLength(originalLength);
                AppendOffset = originalLength;
            }

            throw;
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Добавляет новый элемент в логический конец последовательности.
    /// </summary>
    /// <param name="v">
    ///     Значение элемента, которое нужно добавить.
    /// </param>
    /// <returns>
    ///     Смещение, с которого был записан новый элемент.
    /// </returns>
    /// <remarks>
    ///     Метод использует <see cref="AppendOffset" /> как единственный источник истины
    ///     для позиции добавления.
    ///     После успешной записи увеличивает количество элементов
    ///     и переносит логический конец последовательности на новую позицию.
    ///     Исходная позиция потока после выполнения восстанавливается.
    ///     При ошибке записи выполняется rollback длины файла к предыдущему хвосту.
    /// </remarks>
    public long AppendElement(object v)
    {
        long savedPosition = fs.Position;
        long off = AppendOffset;
        long originalLength = fs.Length;

        try
        {
            EnsureAppendOffsetInvariant();

            fs.Position = off;
            ByteFlow.Serialize(bw, v, tp_elem);

            AppendOffset = fs.Length;
            nelements += 1;

            EnsureAppendOffsetInvariant();
            return off;
        }
        catch
        {
            if (fs.Length != originalLength)
            {
                fs.SetLength(originalLength);
            }

            AppendOffset = originalLength;
            throw;
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Десериализует элемент из текущей позиции потока.
    /// </summary>
    /// <returns>
    ///     Прочитанный элемент.
    /// </returns>
    /// <remarks>
    ///     Это низкоуровневый primitive чтения из текущей позиции.
    ///     Метод не выполняет проверку смещения и не восстанавливает позицию потока;
    ///     эти задачи решаются во внешних wrapper-методах.
    /// </remarks>
    public object GetElement()
    {
        return ByteFlow.Deserialize(br, tp_elem);
    }

    /// <summary>
    ///     Читает элемент по указанному смещению в потоке.
    /// </summary>
    /// <param name="off">
    ///     Смещение начала элемента в потоке.
    /// </param>
    /// <returns>
    ///     Прочитанный элемент.
    /// </returns>
    /// <remarks>
    ///     Метод проверяет, что смещение попадает в логический диапазон последовательности,
    ///     временно устанавливает поток в нужную позицию,
    ///     считывает элемент и затем восстанавливает исходную позицию.
    /// </remarks>
    public object GetElement(long off)
    {
        if (off < 8L || off >= AppendOffset) throw new ArgumentOutOfRangeException(nameof(off));

        long savedPosition = fs.Position;

        try
        {
            if (off != fs.Position) fs.Position = off;

            return GetElement();
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Читает значение указанного типа по заданному смещению в потоке.
    /// </summary>
    /// <param name="tp">
    ///     Тип, в формате которого нужно интерпретировать данные.
    /// </param>
    /// <param name="off">
    ///     Смещение начала данных в потоке.
    /// </param>
    /// <returns>
    ///     Прочитанное значение.
    /// </returns>
    /// <remarks>
    ///     Метод полезен для чтения данных по известному смещению
    ///     в альтернативном типе, отличном от основного типа последовательности.
    ///     После завершения исходная позиция потока восстанавливается.
    /// </remarks>
    public object GetTypedElement(PType tp, long off)
    {
        if (tp == null) throw new ArgumentNullException(nameof(tp));

        if (off < 8L || off >= AppendOffset) throw new ArgumentOutOfRangeException(nameof(off));

        long savedPosition = fs.Position;

        try
        {
            if (off != fs.Position) fs.Position = off;

            return ByteFlow.Deserialize(br, tp);
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Читает элемент по его индексу в последовательности фиксированного размера.
    /// </summary>
    /// <param name="index">
    ///     Ноль-основанный индекс элемента.
    /// </param>
    /// <returns>
    ///     Прочитанный элемент.
    /// </returns>
    /// <remarks>
    ///     Метод доступен только для последовательностей,
    ///     в которых размер каждого элемента фиксирован.
    ///     Смещение вычисляется через <see cref="ElementOffset(long)" />,
    ///     а само чтение выполняется через <see cref="GetElement(long)" />.
    /// </remarks>
    public object GetByIndex(long index)
    {
        if (elem_size <= 0)
            throw new InvalidOperationException("Method cannot be used for sequences with variable-size elements.");

        if (index < 0 || index >= nelements) throw new IndexOutOfRangeException();

        long offset = ElementOffset(index);
        return GetElement(offset);
    }

    /// <summary>
    ///     Последовательно перечисляет все элементы последовательности
    ///     в порядке их хранения.
    /// </summary>
    /// <returns>
    ///     Ленивую последовательность всех элементов.
    /// </returns>
    /// <remarks>
    ///     Метод начинает обход после заголовка потока
    ///     и возвращает элементы по одному.
    ///     Исходная позиция потока после завершения перечисления восстанавливается.
    /// </remarks>
    public IEnumerable<object> ElementValues()
    {
        long savedPosition = fs.Position;
        long count = nelements;

        try
        {
            fs.Position = 8L;

            for (long i = 0; i < count; i++) yield return GetElement();
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Последовательно перечисляет указанное количество элементов,
    ///     начиная с заданного смещения.
    /// </summary>
    /// <param name="offset">
    ///     Смещение первого элемента диапазона.
    /// </param>
    /// <param name="number">
    ///     Количество элементов, которое требуется прочитать.
    /// </param>
    /// <returns>
    ///     Ленивую последовательность элементов заданного диапазона.
    /// </returns>
    /// <remarks>
    ///     Метод удобен для частичного обхода последовательности.
    ///     Он проверяет корректность входных параметров,
    ///     временно устанавливает поток в стартовую позицию
    ///     и после завершения перечисления восстанавливает исходную позицию.
    /// </remarks>
    public IEnumerable<object> ElementValues(long offset, long number)
    {
        if (offset < 8L || offset > AppendOffset) throw new ArgumentOutOfRangeException(nameof(offset));

        if (number < 0) throw new ArgumentOutOfRangeException(nameof(number));
        long savedPosition = fs.Position;

        try
        {
            fs.Position = offset;

            for (long i = 0; i < number; i++) yield return GetElement();
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Основной сканер: быстро пробегаем по элементам, обрабатываем пары (offset, pobject) хендлером, хендлер возвращает
    ///     true
    ///     Последовательно обходит элементы и передаёт обработчику
    ///     смещение и значение каждого элемента.
    /// </summary>
    /// <param name="handler">
    ///     Функция-обработчик, которая получает смещение элемента и само значение
    ///     и возвращает <see langword="true" />, если обход нужно продолжить,
    ///     или <see langword="false" />, если его нужно остановить.
    /// </param>
    /// <remarks>
    ///     Это основной контролируемый метод линейного обхода.
    ///     Позволяет обрабатывать элементы без накопления всей последовательности в памяти
    ///     и поддерживает досрочное завершение.
    ///     Исходная позиция потока после обхода восстанавливается.
    /// </remarks>
    public void Scan(Func<long, object, bool> handler)
    {
        if (handler == null) throw new ArgumentNullException(nameof(handler));

        long count = nelements;
        if (count == 0) return;

        long savedPosition = fs.Position;
        try
        {
            fs.Position = 8L;

            for (long i = 0; i < count; i++)
            {
                long off = fs.Position;
                object element = GetElement();

                bool shouldContinue = handler(off, element);
                if (!shouldContinue) break;
            }
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Перечисляет пары «смещение элемента — значение элемента»
    ///     для всей последовательности.
    /// </summary>
    /// <returns>
    ///     Ленивую последовательность пар,
    ///     где первый компонент — смещение элемента,
    ///     а второй — его значение.
    /// </returns>
    /// <remarks>
    ///     Метод полезен, когда при обходе важно знать не только данные элемента,
    ///     но и их физическое положение в потоке.
    ///     Исходная позиция потока после завершения перечисления восстанавливается.
    /// </remarks>
    public IEnumerable<Tuple<long, object>> ElementOffsetValuePairs()
    {
        long savedPosition = fs.Position;
        long count = nelements;

        try
        {
            fs.Position = 8L;

            for (long i = 0; i < count; i++)
            {
                long off = fs.Position;
                object element = GetElement();

                yield return Tuple.Create(off, element);
            }
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Перечисляет пары «смещение элемента — значение элемента»
    ///     для заданного диапазона последовательности.
    /// </summary>
    /// <param name="offset">
    ///     Смещение первого элемента диапазона.
    /// </param>
    /// <param name="number">
    ///     Количество элементов, которые нужно прочитать.
    /// </param>
    /// <returns>
    ///     Ленивую последовательность пар смещений и значений
    ///     для указанного диапазона.
    /// </returns>
    /// <remarks>
    ///     Метод удобен для частичного обхода последовательности,
    ///     когда требуется одновременно получать и положение элемента в потоке,
    ///     и его значение.
    ///     Исходная позиция потока после завершения перечисления восстанавливается.
    /// </remarks>
    public IEnumerable<Tuple<long, object>> ElementOffsetValuePairs(long offset, long number)
    {
        if (offset < 8L || offset > AppendOffset) throw new ArgumentOutOfRangeException(nameof(offset));

        if (number < 0) throw new ArgumentOutOfRangeException(nameof(number));

        long savedPosition = fs.Position;

        try
        {
            fs.Position = offset;

            for (long i = 0; i < number; i++)
            {
                long off = fs.Position;
                object element = GetElement();

                yield return Tuple.Create(off, element);
            }
        }
        finally
        {
            fs.Position = savedPosition;
        }
    }

    /// <summary>
    ///     Если размер элемента фиксированный и есть функция ключа с целочисленным значением
    ///     Сортирует всю последовательность по 32-битному целочисленному ключу.
    ///     TODO: Вроде S32 вполне может работать для произвольных записей, но только на полном диапазоне.
    /// </summary>
    /// <param name="keyFun">
    ///     Функция, вычисляющая ключ сортировки для каждого элемента.
    /// </param>
    /// <remarks>
    ///     Метод доступен только для последовательностей с фиксированным размером элемента.
    ///     Сначала считывает элементы и их ключи в память,
    ///     затем сортирует их стандартным механизмом массива,
    ///     после чего очищает поток и записывает элементы в новом порядке.
    /// </remarks>
    public void Sort32(Func<object, int> keyFun)
    {
        if (keyFun == null) throw new ArgumentNullException(nameof(keyFun));

        if (!tp_elem.HasNoTail || elem_size <= 0)
            throw new InvalidOperationException("Sort32 is available only for fixed-size element sequences.");

        long count = nelements;
        if (count <= 1) return;

        if (count > int.MaxValue)
            throw new InvalidOperationException("Sort32 cannot handle sequences larger than Int32.MaxValue elements.");

        int[] keys = new int[count];
        object[] records = new object[count];

        long index = 0;
        Scan((off, element) =>
        {
            keys[index] = keyFun(element);
            records[index] = element;
            index++;
            return true;
        });

        Array.Sort(keys, records);

        Clear();

        for (long i = 0; i < records.LongLength; i++) AppendElement(records[i]);

        Flush();
    }

    /// <summary>
    ///     Функция сортировки последовательности с использованием 64-разрядного ключа
    ///     Сортирует всю последовательность по 64-битному целочисленному ключу.
    /// </summary>
    /// <param name="keyFun">
    ///     Функция, вычисляющая ключ сортировки для каждого элемента.
    /// </param>
    /// <remarks>
    ///     Метод доступен только для последовательностей с фиксированным размером элемента.
    ///     Работает аналогично <see cref="Sort32(System.Func{object,int})" />,
    ///     но использует 64-битный ключ сортировки.
    ///     После сортировки последовательность полностью переписывается в отсортированном порядке.
    /// </remarks>
    public void Sort64(Func<object, long> keyFun)
    {
        if (keyFun == null) throw new ArgumentNullException(nameof(keyFun));

        if (!tp_elem.HasNoTail || elem_size <= 0)
            throw new InvalidOperationException("Sort64 is available only for fixed-size element sequences.");

        long count = nelements;
        if (count <= 1) return;

        if (count > int.MaxValue)
            throw new InvalidOperationException("Sort64 cannot handle sequences larger than Int32.MaxValue elements.");

        long[] keys = new long[count];
        object[] records = new object[count];

        long index = 0;
        Scan((off, element) =>
        {
            keys[index] = keyFun(element);
            records[index] = element;
            index++;
            return true;
        });

        Array.Sort(keys, records);

        Clear();

        for (long i = 0; i < records.LongLength; i++) AppendElement(records[i]);

        Flush();
    }

    /// <summary>
    ///     Перезаписывает заголовок потока текущим количеством элементов.
    /// </summary>
    private void WriteHeaderElementCount()
    {
        fs.Position = 0L;
        bw.Write(nelements);
    }

    /// <summary>
    ///     Проверяет основной инвариант последовательности:
    ///     логический конец должен совпадать с физической длиной потока.
    /// </summary>
    private void EnsureAppendOffsetInvariant()
    {
        if (AppendOffset != fs.Length)
            throw new InvalidOperationException(
                "AppendOffset invariant is broken: logical append offset must match the physical stream length.");
    }
}
