package link.rdcn.dacp.optree

import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertThrows}
import org.junit.jupiter.api.Test

class RepositoryClientTest {

  // --- Local Mock Context ---
  class MockFlowExecutionContext extends FlowExecutionContext {
    override def getJobId(): String = "test-job-id"
    override def fairdHome: String = "/tmp/test-home"
    override def pythonHome: String = "/usr/bin/python"
    override def isAsyncEnabled(wrapper: TransformFunctionWrapper): Boolean = false
    override def loadRemoteDataFrame(baseUrl: String, transformOp: TransformOp, credentials: Credentials): Option[DataFrame] = None
    override def getRepositoryClient(): Option[OperatorRepository] = None
    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None
  }

  @Test
  def testConstructor(): Unit = {
    // Just verify instantiation doesn't throw
    assertDoesNotThrow(() => new RepositoryClient("http://localhost", 8080))
  }

  @Test
  def testParseTransformFunctionWrapper_ConnectionFailure(): Unit = {
    // 1. Setup
    // Use a port that is unlikely to be listening to simulate connection refused
    val client = new RepositoryClient("http://127.0.0.1", 65432)
    val mockCtx = new MockFlowExecutionContext()
    val params = new JSONObject()

    // 2. Execute & Verify
    // Since we cannot mock the internal static OperatorClient.connect call easily,
    // we expect an exception when it tries to connect to the bad address.
    assertThrows(classOf[Exception], () => {
      client.parseTransformFunctionWrapper(
        functionName = "test-operator",
        functionVersion = Some("1.0.0"),
        params = params,
        ctx = mockCtx,
        id = "op-id-123"
      )
      () // FIX: Explicitly return Unit to match Executable interface
    }, "Should throw exception when unable to connect to repository server")
  }
}