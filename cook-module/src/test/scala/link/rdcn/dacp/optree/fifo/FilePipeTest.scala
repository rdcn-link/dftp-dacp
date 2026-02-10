package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.{ClosableIterator, DataFrame}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer

class FilePipeTest {

  @TempDir
  var tempDir: Path = _

  /**
   * Local concrete implementation of FilePipe for testing purposes.
   * Uses an in-memory buffer for write/read to avoid OS pipe dependency,
   * but uses real files for create/delete to test lifecycle.
   */
  class MockFilePipe(file: File) extends FilePipe(file) {
    val buffer = new ArrayBuffer[String]()

    // Override create to use simple file creation instead of mkfifo
    override def create(): Unit = {
      if (file.exists()) {
        file.delete()
      }
      file.createNewFile()
    }

    override def write(messages: Iterator[String]): Unit = {
      buffer.appendAll(messages)
    }

    // Fixed: Correctly instantiate ClosableIterator using its constructor parameters
    override def read(): ClosableIterator[String] = {
      new ClosableIterator[String](
        underlying = buffer.iterator,
        onClose = () => { /* No-op for mock */ }
      )
    }

    // Stub implementation
    override def dataFrame(): DataFrame = null
  }

  @Test
  def testCreateAndDelete(): Unit = {
    val file = tempDir.resolve("test_pipe_1").toFile
    val pipe = new MockFilePipe(file)

    // 1. Test Create
    pipe.create()
    assertTrue(file.exists(), "File should be created")

    // 2. Test Delete
    pipe.delete()
    assertFalse(file.exists(), "File should be deleted")
  }

  @Test
  def testWriteAndRead(): Unit = {
    val file = tempDir.resolve("test_pipe_2").toFile
    val pipe = new MockFilePipe(file)
    val data = Seq("line1", "line2", "line3")

    // Write
    pipe.write(data.iterator)

    // Read
    val iterator = pipe.read()
    val result = iterator.toSeq

    assertEquals(3, result.size)
    assertEquals("line1", result.head)
    assertEquals("line3", result.last)

    // Verify close doesn't crash
    iterator.close()
  }

  @Test
  def testCopyToFile(): Unit = {
    // 1. Prepare Source
    val sourceFile = tempDir.resolve("source_pipe").toFile
    val sourcePipe = new MockFilePipe(sourceFile)
    val data = Seq("dataA", "dataB")
    sourcePipe.write(data.iterator)

    // 2. Prepare Destination
    val destFile = tempDir.resolve("dest_pipe").toFile
    val destPipe = new MockFilePipe(destFile)

    // 3. Execute Copy
    // Note: Since MockFilePipe is NOT an instance of RowFilePipe, 
    // it will execute the synchronous copy path in FilePipe.copyToFile
    sourcePipe.copyToFile(destPipe)

    // 4. Verify Destination
    val result = destPipe.read().toSeq
    assertEquals(2, result.size, "Data should be copied to destination pipe")
    assertEquals("dataA", result.head)
    assertEquals("dataB", result.last)
  }

  @Test
  def testPath(): Unit = {
    val file = tempDir.resolve("path_test").toFile
    val pipe = new MockFilePipe(file)

    assertEquals(file.getAbsolutePath, pipe.path, "Path should match file absolute path")
  }
}