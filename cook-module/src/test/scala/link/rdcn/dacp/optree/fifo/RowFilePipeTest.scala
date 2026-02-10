package link.rdcn.dacp.optree.fifo

import link.rdcn.dacp.optree.fifo.RowFilePipe
import link.rdcn.struct.StructType
import link.rdcn.struct.ValueType.StringType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.File
import scala.util.Random

class RowFilePipeTest {

  @TempDir
  var tempDir: File = _
  private var testFile: File = _
  private var pipe: RowFilePipe = _

  @BeforeEach
  def setUp(): Unit = {
    testFile = new File(tempDir, s"test_pipe_${Random.nextLong()}.txt")
    pipe = new RowFilePipe(testFile)
  }

  @Test
  def testWriteAndReadRoundTrip(): Unit = {
    val dataLines = Seq("Hello World", "Line 2", "")

    pipe.write(dataLines.iterator)
    assertTrue(testFile.exists(), "File should exist after write")

    val readIterator = pipe.read()
    val readLines = readIterator.toList
    readIterator.close()

    assertEquals(dataLines, readLines, "Read data does not match written data")
  }

  @Test
  def testDataFrameConversion(): Unit = {
    val dataLines = Seq("line1", "line2")
    pipe.write(dataLines.iterator)

    val df = pipe.dataFrame()
    assertEquals(StructType.empty.add("content", StringType), df.schema, "Schema mismatch")
    assertEquals(2, df.collect().length, "Row count mismatch")
  }
}