package link.rdcn.server.module

import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server._
import link.rdcn.struct._
import link.rdcn.user.UserPrincipal
import org.json.JSONObject
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.File
import java.nio.file.{Files, Path}
import scala.collection.mutable.ArrayBuffer

class BaseDftpModuleTest {

  // --- Local Mocks ---

  case object MockUser extends UserPrincipal { def getName: String = "MockUser" }

  class MockAnchor extends Anchor {
    var hookedEventSource: EventSource = _
    val hookedEventHandlers = new ArrayBuffer[EventHandler]()
    override def hook(service: EventSource): Unit = hookedEventSource = service
    override def hook(service: EventHandler): Unit = hookedEventHandlers.append(service)
  }

  class MockEventHub extends EventHub {
    val eventsFired = new ArrayBuffer[CrossModuleEvent]()
    override def fireEvent(event: CrossModuleEvent): Unit = eventsFired.append(event)
  }

  class MockServerContext extends ServerContext {
    override def getHost() = "mock-host"
    override def getPort() = 1234
    override def getProtocolScheme() = "dftp"
    override def getDftpHome() = None
    override def baseUrl = "dftp://mock-host:1234"
    override def registry(dataframe: DataFrame): DftpTicket = "mock-ticket-df"
    override def registry(blob: Blob): DftpTicket = "mock-ticket-blob"
  }

  // Generic Mock for DftpGetStreamRequest since specific subclasses are gone
  class MockGetStreamRequest(path: String) extends DftpGetStreamRequest {
    override def getRequestPath(): String = path
    override def getRequestURL(): String = s"dftp://mock-host:1234$path"
    override def getUserPrincipal(): UserPrincipal = MockUser
  }

  class MockDftpGetStreamResponse extends DftpGetStreamResponse {
    var errorSent = false
    var errorCode = 0
    var message = ""
    var dataFrameSent: DataFrame = _
    var blobSent: Blob = _

    override def sendError(code: Int, msg: String): Unit = {
      errorSent = true; errorCode = code; message = msg
    }
    override def sendDataFrame(df: DataFrame): Unit = dataFrameSent = df
    override def sendBlob(blob: Blob): Unit = blobSent = blob
  }

  class MockGetStreamMethod extends GetStreamMethod {
    override def accepts(request: DftpGetStreamRequest): Boolean = true
    override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
      // Simple mock behavior: return empty DF
      response.sendDataFrame(DefaultDataFrame(StructType.empty, Iterator.empty))
    }
  }

  // --- Tests ---

  private var moduleToTest: BaseDftpModule = _
  private var mockAnchor: MockAnchor = _
  private var mockEventHub: MockEventHub = _
  implicit private var mockContext: ServerContext = _

  private var actionEventHandler: EventHandler = _

  @TempDir
  var tempDirectory: Path = _

  @BeforeEach
  def setUp(): Unit = {
    moduleToTest = new BaseDftpModule()
    mockAnchor = new MockAnchor()
    mockEventHub = new MockEventHub()
    mockContext = new MockServerContext()

    moduleToTest.init(mockAnchor, mockContext)

    assertNotNull(mockAnchor.hookedEventSource, "init() should hook 1 EventSource")
    assertTrue(mockAnchor.hookedEventHandlers.nonEmpty, "init() should hook EventHandlers")

    mockAnchor.hookedEventSource.init(mockEventHub)

    // BaseDftpModule listens to CollectActionMethodEvent
    actionEventHandler = mockAnchor.hookedEventHandlers.find(_.accepts(new CollectActionMethodEvent(null))).orNull

    // Simulate an external module providing a stream method by handling the event fired by BaseDftpModule
    val getStreamEvent = mockEventHub.eventsFired.find(_.isInstanceOf[CollectGetStreamMethodEvent])
    if (getStreamEvent.isDefined) {
      val collector = getStreamEvent.get.asInstanceOf[CollectGetStreamMethodEvent].collector
      collector.addMethod(new MockGetStreamMethod())
    }
  }

  @Test
  def testActionMethod_GET_HappyPath(): Unit = {
    assertNotNull(actionEventHandler, "BaseDftpModule should register a handler for CollectActionMethodEvent")

    // 1. Collect the ActionMethod provided by BaseDftpModule
    val holder = new Workers[ActionMethod]()
    actionEventHandler.doHandleEvent(new CollectActionMethodEvent(holder))
    val actionMethod = holder.work(s => s, null)
    assertNotNull(actionMethod, "BaseDftpModule should provide an ActionMethod")

    // 2. Mock Request/Response for a GET action
    val params = new JSONObject()
    params.put("type", "SourceOp")
    params.put("dataFrameName", "dftp://host/path/data")

    val request = new DftpActionRequest {
      override def getActionName(): String = "GET"
      override def getRequestParameters(): JSONObject = params
      override def getUserPrincipal(): UserPrincipal = MockUser
    }

    val response = new DftpActionResponse {
      var attachedDfResponse: DataFrameResponse = _
      override def attachStream(dataFrameResponse: DataFrameResponse): Unit = attachedDfResponse = dataFrameResponse
      override def attachStream(blobResponse: BlobResponse): Unit = {}
      override def sendPutDataFrameParameters(json: JSONObject, code: Int): Unit = {}
      override def sendPutBlobParameters(json: JSONObject, code: Int): Unit = {}
      override def sendJSONString(json: String, code: Int): Unit = {}
      override def sendError(errorCode: Int, message: String): Unit = fail(s"Error sent: $errorCode $message")
    }

    // 3. Execute
    actionMethod.doAction(request, response)

    // 4. Verify
    assertNotNull(response.attachedDfResponse, "Should attach a DataFrameResponse")
    assertNotNull(response.attachedDfResponse.getDataFrame, "Attached response should contain a DataFrame")
  }

  @Test
  def testBlobFromFile_Helper(): Unit = {
    val file = tempDirectory.resolve("test.blob").toFile
    Files.write(file.toPath, "content".getBytes)

    // Fixed: Added resourcePath argument
    val blob = Blob.fromFile(file)
    assertNotNull(blob)
  }
}