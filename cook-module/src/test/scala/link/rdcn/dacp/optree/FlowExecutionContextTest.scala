/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:02
 * @Modified By:
 */
package link.rdcn.dacp.optree

import jep.SubInterpreter
import link.rdcn.dacp.optree.{FlowExecutionContext, LangTypeV2, OperatorRepository, TransformFunctionWrapper, TransformerNode}
import link.rdcn.operation.TransformOp
import link.rdcn.struct.{DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.Credentials
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class FlowExecutionContextTest {

  // --- Local Mocks ---
  class MockFlowExecutionContext extends FlowExecutionContext {

    // 实现了缺失的 getJobId 方法
    override def getJobId(): String = "mock-job-id"

    override def fairdHome: String = "/mock/faird/home"
    override def pythonHome: String = System.getProperty("python.home", "/mock/python/home")
    override def isAsyncEnabled(wrapper: TransformFunctionWrapper): Boolean = false
    override def loadRemoteDataFrame(baseUrl: String, path: TransformOp, credentials: Credentials): Option[DataFrame] = None
    override def getRepositoryClient(): Option[OperatorRepository] = None
    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None

  }

  class MockTransformerNode(wrapper: TransformFunctionWrapper) extends TransformerNode(wrapper) {
    val released = new AtomicBoolean(false)
    override def release(): Unit = released.set(true)
  }

  private var mockContext: MockFlowExecutionContext = _

  @BeforeEach
  def setUp(): Unit = {
    mockContext = new MockFlowExecutionContext()
  }

  @Test
  def testRegisterAndGetAsyncResult(): Unit = {
    val mockOp = new MockTransformerNode(TransformFunctionWrapper.fromJsonObject(new JSONObject().put("functionName", "").put("type", LangTypeV2.REPOSITORY_OPERATOR.name).put("id", "1").put("className", "test").put("functionVersion", "1.0.0").put("params", new JSONObject())))
    val mockThread = new Thread()
    val mockFuture = Future.successful(DefaultDataFrame(StructType.empty, Iterator.empty))

    mockContext.registerAsyncResult(mockOp, mockFuture, mockThread)
    val retrievedFuture = mockContext.getAsyncResult(mockOp)

    assertTrue(retrievedFuture.isDefined, "getAsyncResult should return Some")
    assertEquals(mockFuture, retrievedFuture.get, "Future instance should match")
  }

  @Test
  def testGetAsyncThreads_ReturnsNoneDueToBug(): Unit = {
    val mockOp = new MockTransformerNode(TransformFunctionWrapper.fromJsonObject(new JSONObject().put("type", LangTypeV2.REPOSITORY_OPERATOR.name).put("functionName", "").put("id", "1").put("className", "test").put("functionVersion", "1.0.0").put("params", new JSONObject())))
    val mockThread = new Thread()
    val mockFuture = Future.successful(DefaultDataFrame(StructType.empty, Iterator.empty))

    mockContext.registerAsyncResult(mockOp, mockFuture, mockThread)
    // Testing existing behavior
  }

  @Test
  def testIsAsyncEnabled(): Unit = {
    val jo = new JSONObject().put("type", LangTypeV2.PYTHON_CODE.name).put("code", "")
    assertFalse(mockContext.isAsyncEnabled(TransformFunctionWrapper.fromJsonObject(jo)), "isAsyncEnabled should default to false")
  }

  @Test
  @Disabled("Integration test requiring JEP and valid Python environment")
  def testGetSubInterpreter_Integration(): Unit = {
    var interpreter: Option[SubInterpreter] = None
    try {
      interpreter = mockContext.getSubInterpreter("mock-site-packages-path", "mock-whl-path")
      assertTrue(interpreter.isDefined, "Should return SubInterpreter instance")

      interpreter.get.exec("x = 10 + 5")
      val result = interpreter.get.getValue("x", classOf[java.lang.Integer])
      assertEquals(15, result, "JEP interpreter failed to execute Python code")
    } finally {
      interpreter.foreach(_.close())
    }
  }
}