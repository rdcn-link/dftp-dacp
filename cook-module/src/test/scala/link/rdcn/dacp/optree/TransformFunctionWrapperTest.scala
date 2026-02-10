package link.rdcn.dacp.optree

import jep.SharedInterpreter
import link.rdcn.dacp.optree._
import link.rdcn.operation.{SharedInterpreterManager, TransformOp}
import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row, StructType, ValueType}
import link.rdcn.user.Credentials
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{Disabled, Test}

import java.io.File
import java.nio.file.Paths
import scala.collection.JavaConverters._

class TransformFunctionWrapperTest {
  val rows = Seq(Row.fromSeq(Seq(1, 2))).iterator
  val dataFrame = DefaultDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
  val dataFrames = Seq(dataFrame)
  val fairdHome = Paths.get(getClass.getClassLoader.getResource("").toURI()).toString

  // Local Mock Context
  def ctx: FlowExecutionContext = new FlowExecutionContext {
    override val pythonHome: String = System.getProperty("python.home", "")
    override def getSharedInterpreter(): Option[SharedInterpreter] = {
      if (pythonHome.isEmpty) None else Some(SharedInterpreterManager.getInterpreter)
    }
    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None
    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))
    override val fairdHome: String = TransformFunctionWrapperTest.this.fairdHome
    override def loadRemoteDataFrame(baseUrl: String, path: TransformOp, credentials: Credentials): Option[DataFrame] = None

    override def getJobId(): String = ""
  }

  @Test
  @Disabled("Integration test requiring JEP")
  def pythonBinTest(): Unit = {
    val whlPath = Paths.get(fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val pythonBin = PythonBin("normalize", whlPath)
    val df = pythonBin.applyToDataFrames(dataFrames, ctx)
    df.foreach(row => {
      assertEquals(0.33, row._1.asInstanceOf[Double], 0.01, "Normalized col_1 mismatch")
      assertEquals(0.67, row._2.asInstanceOf[Double], 0.01, "Normalized col_2 mismatch")
    })
  }

  @Test
  def javaJarTest(): Unit = {
    import org.json.JSONObject
    val jarName = "dftp-plugin-impl-0.5.0-20250910.jar"
    val jarPath = Paths.get(fairdHome, "lib", "java", jarName).toString

    if (!new File(jarPath).exists()) return

    val javaJar = JavaJar(jarPath, "Transformer11", "", new JSONObject(), "")
    val newDataFrame = javaJar.applyToInput(dataFrames, ctx).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      assertEquals(1, row.getAs[Int](0), "JavaJar col_1 mismatch")
      assertEquals(2, row.getAs[Int](1), "JavaJar col_2 mismatch")
      assertEquals(100, row.getAs[Int](2), "JavaJar col_3 mismatch")
    })
  }

  @Test
  def testPythonCode_Integration(): Unit = {
    // Only works if JEP is active, otherwise might fail or return mock
    if (ctx.getSharedInterpreter().isEmpty) return

    val code = "output_data = [ [r[0] * 2, r[1] * 2] for r in input_data ]"
    val pythonCode = PythonCode(code)

    val newDf = pythonCode.applyToDataFrames(dataFrames, ctx)
    val results = newDf.collect()

    assertEquals(1, results.length, "PythonCode should return 1 row")
    val row = results.head
    assertEquals(2, row.getAs[Int](0), "PythonCode result col_1 mismatch")
    assertEquals(4, row.getAs[Int](1), "PythonCode result col_2 mismatch")
  }

  @Test
  def testJsonRoundTrip_PythonCode(): Unit = {
    val original = PythonCode("print('hello')", 50)
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)
    assertEquals(original.toString(), deserialized.toString(), "PythonCode JSON roundtrip failed")
  }
}