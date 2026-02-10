/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:10
 * @Modified By:
 */
package link.rdcn.dacp.cook

import link.rdcn.dacp.cook.DacpCookModule
import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.server._
import link.rdcn.server.module._
import link.rdcn.struct.{Blob, DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.UserPrincipal
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

class DacpCookModuleTest {

  // --- Local Mocks ---

  case object MockUser extends UserPrincipal { def getName: String = "MockUser" }

  class MockAnchor extends Anchor {
    var hookedEventSource: EventSource = _
    val hookedEventHandlers = new ArrayBuffer[EventHandler]()
    override def hook(service: EventSource): Unit = hookedEventSource = service
    override def hook(service: EventHandler): Unit = hookedEventHandlers.append(service)
  }

  class MockEventHub(handlers: Seq[EventHandler]) extends EventHub {
    val eventsFired = new ArrayBuffer[CrossModuleEvent]()
    override def fireEvent(event: CrossModuleEvent): Unit = {
      handlers.filter(_.accepts(event)).foreach(_.doHandleEvent(event))
      eventsFired.append(event)
    }
  }

  class MockServerContext extends ServerContext {
    override def getHost() = "mock"
    override def getPort() = 0
    override def getProtocolScheme() = "dftp"
    override def getDftpHome() = None
    // Implement missing registry methods required by updated ServerContext
    override def registry(dataframe: DataFrame): DftpTicket = "mock-ticket-df"
    override def registry(blob: Blob): DftpTicket = "mock-ticket-blob"
  }

  // Generic Mock for DftpGetStreamRequest
  class MockDftpGetStreamRequest extends DftpGetStreamRequest {
    override def getRequestPath(): String = "/path"
    override def getRequestURL(): String = "dftp://mock/path"
    override def getUserPrincipal(): UserPrincipal = MockUser
  }

  class MockDftpGetStreamResponse extends DftpGetStreamResponse {
    var dataFrameSent: DataFrame = _
    override def sendError(code: Int, msg: String): Unit = {}
    override def sendDataFrame(df: DataFrame): Unit = dataFrameSent = df
    override def sendBlob(blob: Blob): Unit = {}
  }

  // --- Tests ---

  private var moduleToTest: DacpCookModule = _
  private var mockAnchor: MockAnchor = _
  private var mockEventHub: MockEventHub = _
  implicit private var mockContext: ServerContext = _

  @BeforeEach
  def setUp(): Unit = {
    moduleToTest = new DacpCookModule()
    mockAnchor = new MockAnchor()
    mockContext = new MockServerContext()
    moduleToTest.init(mockAnchor, mockContext)

    mockEventHub = new MockEventHub(mockAnchor.hookedEventHandlers)
    // Init EventSource if it exists
    if (mockAnchor.hookedEventSource != null) {
      mockAnchor.hookedEventSource.init(mockEventHub)
    }
  }

  @Test
  def testInit_FiresAndHooksEvents(): Unit = {
    // Verify module initialization logic
    // DacpCookModule typically hooks DataFrame providers or Auth methods

    if (mockAnchor.hookedEventSource != null) {
      // It should fire CollectDataFrameProviderEvent or similar
      assertTrue(mockEventHub.eventsFired.nonEmpty, "Module should fire events during init")

      // Verify CollectDataFrameProviderEvent is present (common for CookModule)
      val hasProviderEvent = mockEventHub.eventsFired.exists(_.isInstanceOf[CollectGetStreamMethodEvent])
      assertTrue(hasProviderEvent, "Should fire CollectGetStreamMethodEvent")
    }

    // Verify Handlers are hooked
    assertTrue(mockAnchor.hookedEventHandlers.nonEmpty, "Module should hook EventHandlers")
  }
}