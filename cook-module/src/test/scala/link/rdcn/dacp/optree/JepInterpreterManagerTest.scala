/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:02
 * @Modified By:
 */
package link.rdcn.dacp.optree

import jep.{JepException, SubInterpreter}
import link.rdcn.dacp.optree.JepInterpreterManager
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.io.File
import java.nio.file.Paths

class JepInterpreterManagerTest {

  @TempDir
  var tempDir: File = _

  private var sitePackagePath: String = _
  private var validWhlPath: String = _
  private var invalidWhlPath: String = _
  private var pythonHome: Option[String] = None

  @BeforeEach
  def setUp(): Unit = {
    val sitePackageDir = new File(tempDir, "site-packages")
    sitePackageDir.mkdirs()
    sitePackagePath = sitePackageDir.getCanonicalPath

    try {
      val whlUri = getClass.getClassLoader.getResource("lib/link-0.1-py3-none-any.whl").toURI
      validWhlPath = Paths.get(whlUri).toString
    } catch {
      case e: Exception =>
        println(s"Warning: Could not load 'lib/link-0.1-py3-none-any.whl'. Integration test will fail.")
        validWhlPath = "invalid.whl"
    }

    invalidWhlPath = Paths.get(tempDir.getCanonicalPath, "nonexistent-file.whl").toString
    pythonHome = Option(System.getProperty("python.home"))
  }

  @Test
  @Disabled("Integration test: requires Python + Pip + JEP")
  def testGetJepInterpreter_SuccessfulInstallAndRun(): Unit = {
    var interp: SubInterpreter = null
    try {
      interp = JepInterpreterManager.getJepInterpreter(sitePackagePath, validWhlPath, pythonHome)
      assertNotNull(interp, "getJepInterpreter should not return null")

      interp.exec("import link.rdcn.operators.registry as registry")
      interp.exec("x = 10 + 5")
      val result = interp.getValue("x", classOf[java.lang.Integer])
      assertEquals(15, result, "JEP interpreter failed to execute code")
    } finally {
      if (interp != null) interp.close()
    }
  }

  @Test
  @Disabled("Integration test")
  def testGetJepInterpreter_ClassEnquirerWorks(): Unit = {
    var interp: SubInterpreter = null
    try {
      interp = JepInterpreterManager.getJepInterpreter(sitePackagePath, validWhlPath, pythonHome)

      interp.exec("from java.util import ArrayList")

      val ex = assertThrows(classOf[JepException], () => {
        interp.exec("from java.io import File")
      }, "Importing java.io should be blocked")

      assertTrue(ex.getMessage.contains("java.io"), "Exception message should mention 'java.io'")
    } finally {
      if (interp != null) interp.close()
    }
  }

  @Test
  def testGetJepInterpreter_FailsOnBadWhlPath(): Unit = {
    val ex = assertThrows(classOf[RuntimeException], () => {
      JepInterpreterManager.getJepInterpreter(sitePackagePath, invalidWhlPath, pythonHome)
      ()
    }, "Should throw RuntimeException for invalid .whl path")

    assertTrue(ex.getMessage.contains("Nonzero exit value"), "Exception message should indicate pip failure")
  }
}