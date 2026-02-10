package link.rdcn.dacp.catalog

import link.rdcn.server._
import link.rdcn.server.module.{ActionMethod, CollectActionMethodEvent, Workers}
import link.rdcn.struct.{Blob, DataFrame}
import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.user.UserPrincipal
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

class DacpCatalogModuleTest {

  // --- Local Mocks ---

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
    override def getHost(): String = "localhost"
    override def getPort(): Int = 0
    override def getProtocolScheme(): String = "dftp"
    override def getDftpHome(): Option[String] = None
    override def registry(dataframe: DataFrame): DftpTicket = "mock-ticket-df"
    override def registry(blob: Blob): DftpTicket = "mock-ticket-blob"
  }

  // --- Tests ---

  private var moduleToTest: DacpCatalogModule = _
  private var mockAnchor: MockAnchor = _
  private var mockEventHub: MockEventHub = _
  private var mockContext: MockServerContext = _

  @BeforeEach
  def setUp(): Unit = {
    moduleToTest = new DacpCatalogModule()
    mockAnchor = new MockAnchor()
    mockContext = new MockServerContext()

    // Init Module
    moduleToTest.init(mockAnchor, mockContext)

    // Setup EventHub with registered handlers
    mockEventHub = new MockEventHub(mockAnchor.hookedEventHandlers)

    // Init EventSource if the module has one
    if (mockAnchor.hookedEventSource != null) {
      mockAnchor.hookedEventSource.init(mockEventHub)
    }
  }

  @Test
  def testInit_RegistersActionMethod(): Unit = {
    // DacpCatalogModule should listen to CollectActionMethodEvent to register its CatalogActionMethod

    // Create the collector and event
    val workers = new Workers[ActionMethod]()
    val event = new CollectActionMethodEvent(workers)

    // Fire the event (simulate Server startup)
    mockEventHub.fireEvent(event)

    // Verify that an ActionMethod was collected
    // We try to "work" with a dummy task to check if any worker was added
    var workerFound = false
    workers.work(
      runMethod = _ => { workerFound = true; null },
      onFail = null
    )

    // If DacpCatalogModule registered a method, the worker logic should be reachable
    // Note: Since we didn't pass a specific task that matches the "accepts" logic,
    // we assume 'workers' is non-empty.
    // A better check is if we can find a handler for a specific catalog action.

    // verify by trying to handle a mock request
    val mockRequest = new DftpActionRequest {
      override def getActionName(): String = "GET_SCHEMA" // A typical catalog action
      override def getRequestParameters(): JSONObject = new JSONObject()
      override def getUserPrincipal(): UserPrincipal = null
    }

    val foundHandler = workers.work(
      runMethod = method => if (method.accepts(mockRequest)) Some(method) else None,
      onFail = None
    )

    assertTrue(foundHandler.isDefined, "Should find a handler for /getSchema action")
  }
}