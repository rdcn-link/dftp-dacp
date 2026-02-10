package link.rdcn.dacp.catalog

import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.server.ServerContext
import link.rdcn.struct.ValueType.{LongType, RefType, StringType}
import link.rdcn.struct._
import org.apache.jena.rdf.model.Model
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.io.ByteArrayInputStream

class CatalogServiceTest {

  private val BASE_URL = "dftp://test-host:1234"

  // Helper method to create a simple ServerContext mock locally
  private def createMockServerContext(): ServerContext = new ServerContext {
    override def getHost(): String = "mock-host"
    override def getPort(): Int = 9999
    override def getProtocolScheme(): String = "dftp"
    override def getDftpHome(): Option[String] = None
    override def baseUrl: String = "dftp://mock-host:9999"
    override def registry(dataframe: DataFrame): DftpTicket = "mock-ticket-df"
    override def registry(blob: Blob): DftpTicket = "mock-ticket-blob"
  }

  /**
   * Test doListDataSets method.
   * Verifies the Schema and Row content including RDF/XML and JSON fields.
   */
  @Test
  def testDoListDataSets(): Unit = {
    // Localize test data
    val dataSetName = "dataset1"
    val rdfXml = "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"/>"

    // Local anonymous mock implementing only necessary methods
    val mockService = new CatalogService {
      override def listDataSetNames(): List[String] = List(dataSetName)

      override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
        rdfModel.read(new ByteArrayInputStream(rdfXml.getBytes), null, "RDF/XML")
      }

      // Unused methods
      override def accepts(request: CatalogServiceRequest): Boolean = false
      override def getDataFrameMetaData(name: String, model: Model): Unit = {}
      override def listDataFrameNames(dataSetId: String): List[String] = Nil
      override def getDocument(name: String): DataFrameDocument = null
      override def getStatistics(name: String): DataFrameStatistics = null
      override def getSchema(name: String): Option[StructType] = None
      override def getDataFrameTitle(name: String): Option[String] = None
    }

    // Execute
    val df = mockService.doListDataSets()

    // Assertions
    val expectedSchema = StructType.empty
      .add("name", StringType)
      .add("meta", StringType)
      .add("DataSetInfo", StringType)
      .add("dataFrames", RefType)

    assertEquals(expectedSchema, df.schema, "Schema returned by doListDataSets does not match")

    val rows = df.collect()
    assertEquals(1, rows.length, "Row count returned by doListDataSets should be 1")

    val row1 = rows.head
    assertEquals(dataSetName, row1._1, "Column 'name' in the first row does not match")

    // Verify DataSetInfo JSON
    val infoJson = new JSONObject(row1.getAs[String](2))
    assertEquals(dataSetName, infoJson.getString("name"), "Field 'name' in 'DataSetInfo' JSON does not match")

    // Verify Ref link
    assertEquals(s"/dataset/dataset1/dataframes", row1._4.asInstanceOf[URIRef].getUrl,
      "URL in 'dataFrames' (Ref) column does not match")
  }

  /**
   * Test doListHostInfo method.
   */
  @Test
  def testDoListHostInfo(): Unit = {
    val mockService = new CatalogService {
      override def accepts(r: CatalogServiceRequest): Boolean = false
      override def listDataSetNames(): List[String] = Nil
      override def getDataSetMetaData(id: String, m: Model): Unit = {}
      override def getDataFrameMetaData(n: String, m: Model): Unit = {}
      override def listDataFrameNames(id: String): List[String] = Nil
      override def getDocument(n: String): DataFrameDocument = null
      override def getStatistics(n: String): DataFrameStatistics = null
      override def getSchema(n: String): Option[StructType] = None
      override def getDataFrameTitle(n: String): Option[String] = None
    }

    val mockContext = createMockServerContext()

    val df = mockService.doListHostInfo(mockContext)

    val expectedSchema = StructType.empty
      .add("hostInfo", StringType)
      .add("resourceInfo", StringType)

    assertEquals(expectedSchema, df.schema, "Schema returned by doListHostInfo does not match")

    val rows = df.collect()
    assertEquals(1, rows.length, "doListHostInfo should return exactly 1 row")

    // Verify hostInfo JSON (returned as String in DataFrame)
    val hostInfoJson = new JSONObject(rows.head.getAs[String](0))
    assertEquals("mock-host", hostInfoJson.getString("faird.host.position"),
      "Field 'faird.host.position' in hostInfo JSON does not match")

    // Verify resourceInfo JSON
    val resourceInfoJson = new JSONObject(rows.head.getAs[String](1))
    assertTrue(resourceInfoJson.has("cpu.cores"), "resourceInfo JSON should contain 'cpu.cores'")
  }
}