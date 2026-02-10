package link.rdcn.dacp.utils

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.nio.file.{Files, Path, Paths}

class FileUtilsTest {

  @TempDir
  var tempDir: Path = _

  @Test
  def testGetTempDirectory(): Unit = {
    // 1. Prepare inputs
    // Use the temporary directory provided by JUnit as the base directory
    val baseDir = tempDir.toString
    val containerId = "test-container-123"

    // 2. Execute the method
    val resultPathString = FileUtils.getTempDirectory(baseDir, containerId)

    // 3. Verify the result
    val resultPath = Paths.get(resultPathString)

    // Construct expected path: baseDir/container_{id}
    val expectedPath = tempDir.resolve(s"container_$containerId").toAbsolutePath

    assertEquals(expectedPath.toString, resultPathString, "Returned path string should match the expected absolute path structure")

    // Verify side effect: Directory creation
    assertTrue(Files.exists(resultPath), "The directory should be created by the method")
    assertTrue(Files.isDirectory(resultPath), "The created path should be a directory")
  }
}