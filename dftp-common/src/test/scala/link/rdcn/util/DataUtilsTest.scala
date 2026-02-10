/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:54
 * @Modified By:
 */
package link.rdcn.util

import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
 * DataUtils JUnit 5 Test Class
 */
class DataUtilsTest {

  // @TempDir creates a new temporary directory for each test method
  @TempDir
  var tempDir: Path = _

  private var testFile: File = _
  private var subDir: File = _

  @BeforeEach
  def setUp(): Unit = {
    // Prepare some files for testing
    testFile = tempDir.resolve("testfile.txt").toFile
    Files.write(testFile.toPath, "line 1\nline 2\nline 3".getBytes(StandardCharsets.UTF_8))

    subDir = tempDir.resolve("subdir").toFile
    subDir.mkdir()
    Files.write(
      subDir.toPath.resolve("nested.txt"),
      "nested line".getBytes(StandardCharsets.UTF_8)
    )
  }

  // Helper: Create a file in the temp directory
  private def createTempFile(name: String, content: String, dir: File = tempDir.toFile): File = {
    val file = new File(dir, name)
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    file
  }

  // Helper: Create an Excel file locally for testing. No need for external generators.
  private def createTestExcelFile(name: String, isXlsx: Boolean): File = {
    val file = tempDir.resolve(name).toFile
    val wb = if (isXlsx) new XSSFWorkbook() else new HSSFWorkbook()
    val sheet = wb.createSheet("Sheet1")

    // Header
    val headerRow = sheet.createRow(0)
    headerRow.createCell(0).setCellValue("ID")
    headerRow.createCell(1).setCellValue("Name")
    headerRow.createCell(2).setCellValue("Value")
    headerRow.createCell(3).setCellValue("IsActive")
    headerRow.createCell(4).setCellValue("DateNum") // Testing date as Long

    // Data Row 1
    val row1 = sheet.createRow(1)
    row1.createCell(0).setCellValue(101)
    row1.createCell(1).setCellValue("Test 1")
    row1.createCell(2).setCellValue(123.45)
    row1.createCell(3).setCellValue(true)

    val dateCell = row1.createCell(4)
    dateCell.setCellValue(44197.0) // Excel date for 2021-01-01
    val style = wb.createDataFormat()
    dateCell.getCellStyle.setDataFormat(style.getFormat("m/d/yy"))

    // Data Row 2
    val row2 = sheet.createRow(2)
    row2.createCell(0).setCellValue(102)
    row2.createCell(1).setCellValue("Test 2")
    row2.createCell(2).setCellValue(200.0) // Integer-like Double
    row2.createCell(3).setCellValue(false)
    row2.createCell(4, CellType.BLANK) // Empty cell

    val fos = new FileOutputStream(file)
    wb.write(fos)
    fos.close()
    wb.close()
    file
  }

  @Test
  def testGetStructTypeFromMap(): Unit = {
    val map = Map("id" -> 10, "name" -> "Alice", "active" -> true, "score" -> 99.5)
    val schema = DataUtils.getStructTypeFromMap(map)

    val expectedTypes = Map(
      "id" -> IntType,
      "name" -> StringType,
      "active" -> BooleanType,
      "score" -> DoubleType
    )

    assertEquals(4, schema.columns.length, "Schema should contain 4 columns")
    assertTrue(schema.columns.forall(c => expectedTypes(c.name) == c.colType), "All field types in Map should be correctly inferred")
  }

  @Test
  def testGetDataFrameByStream(): Unit = {
    val row1 = Row.fromSeq(Seq(1, "a"))
    val row2 = Row.fromSeq(Seq(2, "b"))

    val df = DataUtils.getDataFrameByStream(Iterator(row1, row2))

    // Verify Schema
    val expectedSchema = StructType(Seq(Column("_1", IntType), Column("_2", StringType)))
    assertEquals(expectedSchema, df.schema, "Schema should be inferred from the first row")

    // Verify Data
    val data = df.collect().toList
    assertEquals(List(row1, row2), data, "DataFrame should contain all data")
  }

  @Test
  def testGetDataFrameByStream_Empty(): Unit = {
    val df = DataUtils.getDataFrameByStream(Iterator.empty)
    assertEquals(StructType.empty, df.schema, "Empty stream should return empty Schema")
    assertTrue(df.collect().toList.isEmpty, "Empty stream should return empty data")
  }

  @Test
  def testListAllFilesWithAttrs(): Unit = {
    // testFile (root) and nested.txt (subdir) should both be found
    val files = DataUtils.listAllFilesWithAttrs(tempDir.toFile).toList
    val fileNames = files.map(_._1.getName).toSet

    assertEquals(2, files.length, "Should recursively find 2 files")
    assertEquals(Set("testfile.txt", "nested.txt"), fileNames, "Should contain files from root and subdirectories")

    // Check attributes
    val (_, attrs) = files.head
    assertNotNull(attrs, "File attributes should not be null")
    assertTrue(attrs.isRegularFile, "Should be a regular file")
  }

  @Test
  def testListFilesWithAttributes(): Unit = {
    // Should only find testfile.txt and subdir
    val files = DataUtils.listFilesWithAttributes(tempDir.toFile)
    val fileNames = files.map(_._1.getName).toSet

    assertEquals(2, files.length, "Should non-recursively find 2 entries (1 file, 1 dir)")
    assertEquals(Set("testfile.txt", "subdir"), fileNames, "Should contain root entries")
  }

  @Test
  def testListFilesWithAttributes_NonExistentDir(): Unit = {
    val nonExistentDir = new File(tempDir.toFile, "nonexistent")
    val files = DataUtils.listFilesWithAttributes(nonExistentDir)
    assertTrue(files.isEmpty, "Should return empty Seq for non-existent directory")
  }

  @Test
  def testGetFileLines_Closable(): Unit = {
    val linesIter = DataUtils.getFileLines(testFile)
    val lines = linesIter.toList
    linesIter.close() // Ensure close can be called
    assertEquals(List("line 1", "line 2", "line 3"), lines, "Should read all lines from file")
  }

  @Test
  def testConvertStringRowToTypedRow(): Unit = {
    val schema = StructType(Seq(
      Column("i", IntType),
      Column("l", LongType),
      Column("d", DoubleType),
      Column("f", FloatType),
      Column("b", BooleanType),
      Column("s", StringType),
      Column("n", StringType) // Test null
    ))

    val stringRow = Row.fromSeq(Seq("123", "9876543210", "123.45", "0.5", "true", "hello", null))

    val typedRow = DataUtils.convertStringRowToTypedRow(stringRow, schema)

    val expected = Row.fromSeq(Seq(123, 9876543210L, 123.45, 0.5f, true, "hello", null))

    assertEquals(expected.values, typedRow.values, "All String types should be converted to types specified in Schema")
  }

  @Test
  def testConvertStringRowToTypedRow_Unsupported(): Unit = {
    val schema = StructType(Seq(Column("a", RefType)))
    val row = Row.fromSeq(Seq("some_ref"))

    val exception = assertThrows(classOf[UnsupportedOperationException], () => {
      DataUtils.convertStringRowToTypedRow(row, schema)
      ()
    }, "Should throw exception for unsupported type conversion")
  }

  @Test
  def testInferSchema(): Unit = {
    val header = Seq(" id ", "  value  ", "  flag  ", "mixed") // Test trim
    val lines = Seq(
      Array("1", "10.5", "true", "100"),
      Array("2", "20.0", "false", "text"), // "text" will promote 'mixed' column to String
      Array("3", "5", "true", "2.5")        // 5 (Long) will be promoted too
    )

    val schema = DataUtils.inferSchema(lines, header)

    val expected = StructType(Seq(
      Column("id", LongType),    // 1, 2, 3 -> Long
      Column("value", DoubleType), // 10.5, 20.0, 5 -> Double
      Column("flag", BooleanType), // true, false, true -> Boolean
      Column("mixed", StringType)  // 100 (Long), text (String), 2.5 (Double) -> String
    ))

    assertEquals(expected, schema, "Schema should be correctly inferred and types promoted")
  }

  @Test
  def testInferSchema_NoHeader(): Unit = {
    val lines = Seq(Array("true", "1234567890"), Array("false", "1.5"))
    val schema = DataUtils.inferSchema(lines, Seq.empty)

    val expected = StructType(Seq(
      Column("_1", BooleanType),
      Column("_2", DoubleType) // Long + Double -> Double
    ))

    assertEquals(expected, schema, "Should automatically generate column names _1, _2... when no header provided")
  }

  @Test
  def testInferStringValueType(): Unit = {
    assertAll("inferStringValueType",
      () => assertEquals(LongType, DataUtils.inferStringValueType("12345"), "Should be LongType"),
      () => assertEquals(LongType, DataUtils.inferStringValueType("-10"), "Should be LongType (negative)"),
      () => assertEquals(DoubleType, DataUtils.inferStringValueType("123.45"), "Should be DoubleType"),
      () => assertEquals(DoubleType, DataUtils.inferStringValueType("-0.5"), "Should be DoubleType (negative)"),
      () => assertEquals(BooleanType, DataUtils.inferStringValueType("true"), "Should be BooleanType"),
      () => assertEquals(BooleanType, DataUtils.inferStringValueType("FALSE"), "Should be BooleanType (case insensitive)"),
      () => assertEquals(StringType, DataUtils.inferStringValueType("hello"), "Should be StringType"),
      () => assertEquals(StringType, DataUtils.inferStringValueType("10.5.1"), "Should be StringType (invalid number)"),
      () => assertEquals(StringType, DataUtils.inferStringValueType(""), "Should be StringType (empty)"),
      () => assertEquals(StringType, DataUtils.inferStringValueType(null), "Should be StringType (null)")
    )
  }

  @Test
  def testCountLinesFast(): Unit = {
    val count = DataUtils.countLinesFast(testFile)
    assertEquals(3L, count, "File should have 3 lines")

    val emptyFile = createTempFile("empty.txt", "")
    val emptyCount = DataUtils.countLinesFast(emptyFile)
    assertEquals(0L, emptyCount, "Empty file should have 0 lines")
  }

  @Test
  def testInferSchemaFromRow(): Unit = {
    val row = Row.fromSeq(Seq(10, "hello", true, 1.5, null, 123L))
    val schema = DataUtils.inferSchemaFromRow(row)
    val expected = StructType(Seq(
      Column("_1", IntType),
      Column("_2", StringType),
      Column("_3", BooleanType),
      Column("_4", DoubleType),
      Column("_5", NullType),
      Column("_6", LongType)
    ))
    assertEquals(expected, schema, "Should infer Schema based on values in Row")
  }

  @Test
  def testInferValueType(): Unit = {
    assertAll("inferValueType",
      () => assertEquals(NullType, DataUtils.inferValueType(null), "null -> NullType"),
      () => assertEquals(IntType, DataUtils.inferValueType(123), "Int -> IntType"),
      () => assertEquals(LongType, DataUtils.inferValueType(123L), "Long -> LongType"),
      () => assertEquals(DoubleType, DataUtils.inferValueType(123.45), "Double -> DoubleType"),
      () => assertEquals(DoubleType, DataUtils.inferValueType(123.45f), "Float -> DoubleType"),
      () => assertEquals(BooleanType, DataUtils.inferValueType(true), "Boolean -> BooleanType"),
      () => assertEquals(BinaryType, DataUtils.inferValueType(Array[Byte](1, 2)), "Array[Byte] -> BinaryType"),
      () => assertEquals(BinaryType, DataUtils.inferValueType(new File("a")), "File -> BinaryType"),
      () => assertEquals(BinaryType, DataUtils.inferValueType(Blob.fromFile(new File(""))), "Blob -> BinaryType"),
      () => assertEquals(RefType, DataUtils.inferValueType(RefType), "URIRef -> RefType"),
      () => assertEquals(StringType, DataUtils.inferValueType("text"), "String -> StringType"),
      () => assertEquals(StringType, DataUtils.inferValueType(new java.util.Date()), "Other -> StringType")
    )
  }

  @Test
  def testInferExcelSchema_Xlsx(): Unit = {
    val excelFile = createTestExcelFile("test.xlsx", isXlsx = true)
    val schema = DataUtils.inferExcelSchema(excelFile.getAbsolutePath)

    val expected = StructType(Seq(
      Column("ID", LongType),      // 101
      Column("Name", StringType),  // "Test 1"
      Column("Value", LongType), // 123.45
      Column("IsActive", BooleanType), // true
      Column("DateNum", LongType) // Date detected as Long
    ))

    assertEquals(expected, schema, "XLSX Schema should be correctly inferred")
  }

  @Test
  def testInferExcelSchema_Empty(): Unit = {
    val wb = new XSSFWorkbook()
    wb.createSheet("Sheet1")
    val file = tempDir.resolve("empty.xlsx").toFile
    val fos = new FileOutputStream(file)
    wb.write(fos)
    fos.close()
    wb.close()

    assertThrows(classOf[RuntimeException], () => {
      DataUtils.inferExcelSchema(file.getAbsolutePath)
      ()
    }, "Empty Excel file should throw exception")
  }

  @Test
  def testReadExcelRows_Xlsx(): Unit = {
    val excelFile = createTestExcelFile("read.xlsx", isXlsx = true)
    val schema = StructType(Seq(
      Column("ID", IntType),
      Column("Name", StringType),
      Column("Value", DoubleType),
      Column("IsActive", BooleanType),
      Column("DateNum", LongType) // Intentionally read date as Long
    ))

    val rows = DataUtils.readExcelRows(excelFile.getAbsolutePath, schema).toList

    assertEquals(2, rows.length, "Should read 2 rows of data")

    // Check first row
    assertEquals(List(101, "Test 1", 123.45, true, 44197L), rows.head, "First row data should be read correctly")
    // Check second row
    assertEquals(List(102, "Test 2", 200.0, false, ""), rows(1), "Second row data should be read correctly (empty cell to empty string)")
  }

  @Test
  def testReadExcelRows_Xls(): Unit = {
    val excelFile = createTestExcelFile("read.xls", isXlsx = false)
    val schema = StructType(Seq(
      Column("ID", IntType),
      Column("Name", StringType),
      Column("Value", DoubleType)
    ))

    val rows = DataUtils.readExcelRows(excelFile.getAbsolutePath, schema).toList

    assertEquals(2, rows.length, "Should read 2 rows of .xls data")
    assertEquals(List(101, "Test 1", 123.45), rows.head.take(3), "First row of .xls data should be read correctly")
    assertEquals(List(102, "Test 2", 200.0), rows(1).take(3), "Second row of .xls data should be read correctly")
  }

  @Test
  def testGetStructTypeStreamFromJson(): Unit = {
    val json1 = "{\"id\": 1, \"name\": \"A\"}"
    val json2 = "{\"id\": 2, \"name\": \"B\"}"
    val iter = Iterator(json1, json2)

    val (stream, schema) = DataUtils.getStructTypeStreamFromJson(iter)

    // Verify Schema
    val expectedSchema = StructType(Seq(Column("name", StringType), Column("id", IntType)))
    assertEquals(expectedSchema, schema, "JSON Schema should be correctly inferred from first row")

    // Verify stream data
    val rows = stream.toList
    assertEquals(2, rows.length, "Should contain all rows")
    assertEquals(Row.fromJsonString(json2).toString, rows.head.toString, "First element of stream should be the second element of original iterator")
    assertEquals(Row.fromJsonString(json1).toString, rows(1).toString, "Last element of stream should be the first element of original iterator")
  }

  @Test
  def testGetStructTypeStreamFromJson_Empty(): Unit = {
    val (stream, schema) = DataUtils.getStructTypeStreamFromJson(Iterator.empty)
    assertEquals(StructType.empty, schema, "Empty iterator should return empty Schema")
    assertTrue(stream.isEmpty, "Empty iterator should return empty stream")
  }

  @Test
  def testChunkedIterator(): Unit = {
    val data = Array.tabulate[Byte](25)(i => i.toByte) // 25 bytes
    val is = new ByteArrayInputStream(data)

    val iter = DataUtils.chunkedIterator(is, 10) // chunk size 10

    val chunks = iter.toList

    assertEquals(3, chunks.length, "Should have 3 chunks")
    assertEquals(10, chunks(0).length, "Chunk 1 size should be 10")
    assertEquals(10, chunks(1).length, "Chunk 2 size should be 10")
    assertEquals(5, chunks(2).length, "Chunk 3 size should be 5 (remainder)")

    // Verify content
    assertEquals(data.slice(0, 10).toList, chunks(0).toList, "Chunk 1 content should be correct")
    assertEquals(data.slice(20, 25).toList, chunks(2).toList, "Chunk 3 content should be correct")
  }

  @Test
  def testChunkedIterator_EmptyInput(): Unit = {
    val is = new ByteArrayInputStream(Array[Byte]())
    val iter = DataUtils.chunkedIterator(is, 10)

    // Using hasNext checks before calling next
    // assertFalse(iter.hasNext, "Empty input stream hasNext should be false")
    // Depending on implementation, assertThrows might be more appropriate if next() is called directly
    assertThrows(classOf[NoSuchElementException], () => {
      iter.next()
      ()
    }, "Empty input stream should throw NoSuchElementException when next() is called")
  }
}