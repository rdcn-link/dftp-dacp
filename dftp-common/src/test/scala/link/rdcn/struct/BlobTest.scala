/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class BlobTest {

  @TempDir
  var tempDir: Path = _

  @Test
  def testBlobFromFile(): Unit = {
    // Create a temporary file
    val content = "Blob test content 123"
    val file = tempDir.resolve("test.blob").toFile
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))

    // Create Blob from file
    val blob = Blob.fromFile(file)
    assertNotNull(blob, "Blob creation failed")

    // Consume stream and verify
    blob.offerStream { is =>
      val bytes = new Array[Byte](is.available())
      is.read(bytes)
      val readContent = new String(bytes, StandardCharsets.UTF_8)

      assertEquals(content, readContent, "Content read from Blob stream mismatch")
    }
  }
}