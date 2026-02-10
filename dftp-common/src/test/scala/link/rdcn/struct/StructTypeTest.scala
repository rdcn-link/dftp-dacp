/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:54
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.struct.ValueType._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class StructTypeTest {

  val baseSchema = StructType.fromNamesAndTypes(
    "id" -> IntType,
    "name" -> StringType,
    "active" -> BooleanType
  )

  @Test
  def testConstructor_DuplicateNamesFail(): Unit = {
    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromNamesAndTypes(
        "id" -> IntType,
        "name" -> StringType,
        "id" -> BooleanType // Duplicate
      )
      ()
    }, "StructType constructor should throw exception on duplicate column names")

    assertTrue(ex.getMessage.contains("id"), "Exception message should indicate duplicated column name")
  }

  @Test
  def testIsEmptyAndEmpty(): Unit = {
    assertTrue(StructType.empty.isEmpty(), "StructType.empty.isEmpty() should be true")
    assertFalse(baseSchema.isEmpty(), "baseSchema.isEmpty() should be false")
  }

  @Test
  def testGetType(): Unit = {
    assertEquals(Some(IntType), baseSchema.getType("id"), "getType('id') should be Some(IntType)")
    assertEquals(Some(StringType), baseSchema.getType("name"), "getType('name') should be Some(StringType)")
    assertEquals(None, baseSchema.getType("nonexistent"), "getType('nonexistent') should be None")
  }

  @Test
  def testContains(): Unit = {
    assertTrue(baseSchema.contains("id"), "contains('id') should be true")
    assertFalse(baseSchema.contains("nonexistent"), "contains('nonexistent') should be false")

    assertTrue(baseSchema.contains(IntType), "contains(IntType) should be true")
    assertFalse(baseSchema.contains(FloatType), "contains(FloatType) should be false")
  }

  @Test
  def testColumnNames(): Unit = {
    assertEquals(Seq("id", "name", "active"), baseSchema.columnNames, "columnNames Seq mismatch")
  }

  @Test
  def testIndexOf(): Unit = {
    assertEquals(Some(0), baseSchema.indexOf("id"), "indexOf('id') should be Some(0)")
    assertEquals(Some(1), baseSchema.indexOf("name"), "indexOf('name') should be Some(1)")
    assertEquals(Some(2), baseSchema.indexOf("active"), "indexOf('active') should be Some(2)")
    assertEquals(None, baseSchema.indexOf("nonexistent"), "indexOf('nonexistent') should be None")
  }

  @Test
  def testColumnAt(): Unit = {
    assertEquals(Column("id", IntType, true), baseSchema.columnAt(0), "columnAt(0) mismatch")
    assertEquals(Column("name", StringType, true), baseSchema.columnAt(1), "columnAt(1) mismatch")

    // Test OOB
    assertThrows(classOf[IndexOutOfBoundsException], () => {
      baseSchema.columnAt(99)
      ()
    }, "columnAt(99) should throw IndexOutOfBoundsException")
  }

  @Test
  def testAdd(): Unit = {
    val newSchema = baseSchema.add("salary", DoubleType, nullable = false)

    // Verify immutability
    assertEquals(3, baseSchema.columns.length, "Original schema should not be modified")

    // Verify new schema
    assertEquals(4, newSchema.columns.length, "New schema length incorrect after add()")
    assertEquals(Column("salary", DoubleType, false), newSchema.columnAt(3), "Added column definition incorrect")
    assertEquals(Some(3), newSchema.indexOf("salary"), "Added column index incorrect")
  }

  @Test
  def testSelect(): Unit = {
    // 1. Success case
    val selectedSchema = baseSchema.select("name", "id")

    assertEquals(2, selectedSchema.columns.length, "Selected schema length incorrect")
    assertEquals(Column("name", StringType, true), selectedSchema.columnAt(0), "First selected column incorrect")
    assertEquals(Column("id", IntType, true), selectedSchema.columnAt(1), "Second selected column incorrect")

    // 2. Failure case
    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      baseSchema.select("id", "nonexistent")
      ()
    }, "select() should throw exception for non-existent columns")

    assertTrue(ex.getMessage.contains("nonexistent"), "Exception message should indicate missing column")
  }

  @Test
  def testPrependAndAppend(): Unit = {
    val headCol = Column("newHead", BinaryType)
    val tailCol = Column("newTail", BinaryType)

    // 1. Test prepend
    val prepended = baseSchema.prepend(headCol)
    assertNotSame(baseSchema, prepended, "prepend should return new instance")
    assertEquals(4, prepended.columns.length, "Length incorrect after prepend")
    assertEquals(headCol, prepended.columnAt(0), "First column incorrect after prepend")
    assertEquals("id", prepended.columnAt(1).name, "Original column index shift incorrect")

    // 2. Test append
    val appended = baseSchema.append(tailCol)
    assertNotSame(baseSchema, appended, "append should return new instance")
    assertEquals(4, appended.columns.length, "Length incorrect after append")
    assertEquals(tailCol, appended.columnAt(3), "Last column incorrect after append")
  }

  @Test
  def testToString(): Unit = {
    val expected = "schema(id: Int, name: String, active: Boolean)"
    assertEquals(expected, baseSchema.toString, "toString() format mismatch")

    val emptyExpected = "schema()"
    assertEquals(emptyExpected, StructType.empty.toString, "Empty schema toString() format mismatch")
  }

  // --- Test object StructType factory methods ---

  @Test
  def testFactory_FromColumns(): Unit = {
    val cols = Seq(Column("a", IntType), Column("b", StringType))
    val schema = StructType.fromColumns(cols: _*)
    assertEquals(cols, schema.columns, "fromColumns failed")
  }

  @Test
  def testFactory_FromSeq(): Unit = {
    val cols = Seq(Column("a", IntType), Column("b", StringType))
    val schema = StructType.fromSeq(cols)
    assertEquals(cols, schema.columns, "fromSeq failed")
  }

  @Test
  def testFactory_FromNamesAndTypes(): Unit = {
    val expectedCols = Seq(Column("a", IntType), Column("b", StringType))
    val schema = StructType.fromNamesAndTypes("a" -> IntType, "b" -> StringType)
    assertEquals(expectedCols, schema.columns, "fromNamesAndTypes failed")
  }

  @Test
  def testFactory_FromNamesAsAny(): Unit = {
    val expectedCols = Seq(Column("a", StringType), Column("b", StringType))
    val schema = StructType.fromNamesAsAny(Seq("a", "b"))
    assertEquals(expectedCols, schema.columns, "fromNamesAsAny failed")
  }

  @Test
  def testFactory_FromString_HappyPath(): Unit = {
    val schemaString = "schema(id: Int, name: String, active: Boolean)"
    val schema = StructType.fromString(schemaString)

    assertEquals(baseSchema.columns, schema.columns, "Schema parsed from string mismatch")
  }

  @Test
  def testFactory_FromString_EmptyCases(): Unit = {
    assertEquals(StructType.empty.columns, StructType.fromString("schema()").columns, "fromString('schema()') should be empty")
    assertEquals(StructType.empty.columns, StructType.fromString("schema(   )").columns, "fromString('schema( )') should be empty")
    assertEquals(StructType.empty.columns, StructType.fromString("").columns, "fromString('') should be empty")
  }

  @Test
  def testFactory_FromString_FailureCases(): Unit = {
    // 1. Missing prefix
    assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromString("id: IntType)")
      ()
    }, "fromString() should throw exception without 'schema(' prefix")

    // 2. Missing suffix
    assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromString("schema(id: IntType")
      ()
    }, "fromString() should throw exception without ')' suffix")

    // 3. Invalid column format
    val exCol = assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromString("schema(id IntType)") // Missing ':'
      ()
    }, "fromString() should throw exception on invalid column format")
    assertTrue(exCol.getMessage.contains("id IntType"), "Exception message should indicate invalid column part")

    // 4. Unknown type
    assertThrows(classOf[Exception], () => {
      StructType.fromString("schema(id: UnknownType)")
      ()
    }, "fromString() should throw exception on unknown type")
  }

  @Test
  def testStandardSchemas(): Unit = {
    // 1. blobStreamStructType
    val blobSchema = StructType.blobStreamStructType
    assertEquals(1, blobSchema.columns.length, "blobStreamStructType should have 1 column")
    assertEquals(Column("content", BinaryType), blobSchema.columnAt(0), "blobStreamStructType column definition incorrect")

    // 2. binaryStructType
    val binarySchema = StructType.binaryStructType
    assertEquals(7, binarySchema.columns.length, "binaryStructType should have 7 columns")
    assertEquals(Column("name", StringType), binarySchema.columnAt(0), "binaryStructType 'name' column incorrect")
    assertEquals(Column("File", BinaryType), binarySchema.columnAt(6), "binaryStructType 'File' column incorrect")
  }
}