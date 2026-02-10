package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.struct.ValueType.StringType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Path

class FileDataFrameTest {

  @TempDir
  var tempDir: Path = _

  /**
   * Local Mock for FilePipe to simulate returning a DataFrame.
   * We extend FilePipe and override dataFrame() to return our test fixture.
   */
  class MockFilePipe(file: File, dfToReturn: DataFrame) extends FilePipe(file) {
    override def create(): Unit = {}
    override def write(messages: Iterator[String]): Unit = {}
    override def read(): ClosableIterator[String] = ClosableIterator(Iterator.empty)(())
    override def delete(): Unit = {}

    // Key method for FileDataFrame
    override def dataFrame(): DataFrame = dfToReturn
  }

  @Test
  def testFileDataFrameDelegation(): Unit = {
    // 1. Prepare Underlying Data
    val testSchema = StructType.empty.add("content", StringType)
    val testRows = Seq(Row("line1"), Row("line2"))
    val underlyingDf = DefaultDataFrame(testSchema, testRows.iterator)

    // 2. Mock the Pipe
    val dummyFile = tempDir.resolve("test.pipe").toFile
    val mockPipe = new MockFilePipe(dummyFile, underlyingDf)

    // 3. Initialize FileDataFrame
    val fileDf = FileDataFrame(mockPipe, FileType.FIFO_BUFFER)

    // 4. Verify Schema
    // FileDataFrame hardcodes schema to StructType.empty.add("content", StringType)
    assertEquals("content", fileDf.schema.columns.head.name)
    assertEquals(StringType, fileDf.schema.columns.head.colType)

    // 5. Verify Data Collection (Delegation)
    val collected = fileDf.collect()
    assertEquals(2, collected.length)
    assertEquals("line1", collected(0).getAs[String](0))
    assertEquals("line2", collected(1).getAs[String](0))
  }

  @Test
  def testFileTypeEnum(): Unit = {
    // Test toString
    assertEquals("FIFO_BUFFER", FileType.FIFO_BUFFER.toString)
    assertEquals("RAM_FILE", FileType.RAM_FILE.toString)
    assertEquals("MMAP_FILE", FileType.MMAP_FILE.toString)
    assertEquals("DIRECTORY", FileType.DIRECTORY.toString)

    // Test fromString
    assertEquals(FileType.FIFO_BUFFER, FileType.fromString("FIFO_BUFFER"))
    assertEquals(FileType.RAM_FILE, FileType.fromString("ram_file")) // Case insensitive check in impl usually?
    // Code says: str.toUpperCase match ... so "ram_file" -> "RAM_FILE" -> Match

    assertEquals(FileType.DIRECTORY, FileType.fromString("Directory"))

    // Test Invalid
    assertThrows(classOf[NoSuchElementException], () => {
      FileType.fromString("INVALID_TYPE")
    })
  }
}