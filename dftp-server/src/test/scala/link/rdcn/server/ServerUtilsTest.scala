/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.server

import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import org.apache.arrow.flight.FlightStream.{Cancellable, Requestor}
import org.apache.arrow.flight.{FlightProducer, FlightStream, Result}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.types.{FloatingPointPrecision, Types}
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeEach, Test}

import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.collection.mutable.ArrayBuffer

object ServerUtilsTest {
  private var allocator: RootAllocator = _

  @AfterAll
  def tearDown(): Unit = {
    if (allocator != null) allocator.close()
  }
}

class ServerUtilsTest {

  @BeforeEach
  def setUp(): Unit = {
    ServerUtilsTest.allocator = new RootAllocator(Long.MaxValue)
  }

  @Test
  def testConvertStructTypeToArrowSchema_PrimitiveTypes(): Unit = {
    val structType = StructType(List(
      Column("c1", IntType, nullable = false),
      Column("c2", LongType),
      Column("c3", FloatType),
      Column("c4", DoubleType),
      Column("c5", BooleanType),
      Column("c6", StringType),
      Column("c7", BinaryType)
    ))

    val arrowSchema = ServerUtils.convertStructTypeToArrowSchema(structType)
    val fields = arrowSchema.getFields.asScala.toList

    assertEquals(7, fields.size, "Schema should have 7 fields")

    // Verify IntType
    assertEquals(Types.MinorType.INT.getType, fields(0).getType, "Field c1 type should be INT")
    assertTrue(!fields(0).isNullable, "Field c1 should be non-nullable")

    // Verify DoubleType
    assertTrue(fields(3).getType.isInstanceOf[ArrowType.FloatingPoint], "Field c4 type should be FloatingPoint")
    assertEquals(FloatingPointPrecision.DOUBLE, fields(3).getType.asInstanceOf[ArrowType.FloatingPoint].getPrecision, "Field c4 precision should be DOUBLE")

    // Verify StringType
    assertEquals(ArrowType.Utf8.INSTANCE, fields(5).getType, "Field c6 type should be Utf8")
  }

  @Test
  def testConvertStructTypeToArrowSchema_LogicalTypes(): Unit = {
    val structType = StructType(List(
      Column("c1", RefType),
      Column("c2", BinaryType)
    ))

    val arrowSchema = ServerUtils.convertStructTypeToArrowSchema(structType)
    val fields = arrowSchema.getFields.asScala.toList

    // Verify RefType
    assertEquals(ArrowType.Utf8.INSTANCE, fields(0).getType, "RefType should be Utf8")
    assertEquals("Url", fields(0).getMetadata.get("logicalType"), "RefType metadata should contain 'Url'")

    // Verify BlobType
    assertEquals(new ArrowType.Binary(), fields(1).getType, "BlobType should be Binary")
    assertEquals("blob", fields(1).getMetadata.get("logicalType"), "BlobType metadata should contain 'blob'")
  }

  @Test
  def testArrowSchemaToStructType_AllTypes(): Unit = {
    val refMetadata = new java.util.HashMap[String, String]()
    refMetadata.put("logicalType", "Url")
    val blobMetadata = new java.util.HashMap[String, String]()
    blobMetadata.put("logicalType", "blob")

    val arrowSchema = new Schema(List(
      Field.nullable("i", Types.MinorType.INT.getType),
      Field.nullable("l", Types.MinorType.BIGINT.getType),
      Field.nullable("f4", Types.MinorType.FLOAT4.getType),
      Field.nullable("f8", Types.MinorType.FLOAT8.getType),
      Field.nullable("s", Types.MinorType.VARCHAR.getType),
      Field.nullable("b", Types.MinorType.BIT.getType),
      Field.nullable("bin", Types.MinorType.VARBINARY.getType),
      new Field("ref", new FieldType(true, Types.MinorType.VARCHAR.getType, null, refMetadata), Collections.emptyList()),
      new Field("blob", new FieldType(true, Types.MinorType.VARBINARY.getType, null, blobMetadata), Collections.emptyList())
    ).asJava)

    val structType = ServerUtils.arrowSchemaToStructType(arrowSchema)
    val columns = structType.columns

    assertEquals(IntType, columns(0).colType, "Arrow INT should map to IntType")
    assertEquals(LongType, columns(1).colType, "Arrow BIGINT should map to LongType")
    assertEquals(FloatType, columns(2).colType, "Arrow FLOAT4 should map to FloatType")
    assertEquals(DoubleType, columns(3).colType, "Arrow FLOAT8 should map to DoubleType")
    assertEquals(StringType, columns(4).colType, "Arrow VARCHAR without metadata should map to StringType")
    assertEquals(BooleanType, columns(5).colType, "Arrow BIT should map to BooleanType")
    assertEquals(BinaryType, columns(6).colType, "Arrow VARBINARY without metadata should map to BinaryType")
    assertEquals(RefType, columns(7).colType, "Arrow VARCHAR with metadata should map to RefType")
    assertEquals(BinaryType, columns(8).colType, "Arrow VARBINARY with metadata should map to BlobType")
  }

  @Test
  def testArrowSchemaToStructType_UnsupportedType(): Unit = {
    val unsupportedType = Types.MinorType.DATEDAY.getType // Use DATEDAY as unsupported
    val arrowSchema = new Schema(Collections.singletonList(
      Field.nullable("c1", unsupportedType)
    ))

    val exception = assertThrows(
      classOf[UnsupportedOperationException],
      () => ServerUtils.arrowSchemaToStructType(arrowSchema)
    )

    assertEquals(s"Unsupported Arrow type: ${unsupportedType}", exception.getMessage, "Exception message should indicate unsupported Arrow type")
  }

  @Test
  def testFlightStreamToRowIterator(): Unit = {
    val row1 = Row("data1")
    val row2 = Row("data2")
    val row3 = Row("data3")
    val allRows = List(row1, row2, row3)

    val mockStream = new MockFlightStream(allRows, ServerUtilsTest.allocator)
    val rowIterator = ServerUtils.flightStreamToRowIterator(mockStream)

    // Check hasNext logic
    assertTrue(rowIterator.hasNext, "Iterator should have next batch")

    // Check next() logic
    val r1 = rowIterator.next()
    assertEquals(row1.values.head, r1.values.head.asInstanceOf[ArrayBuffer[String]].head, "First row content should match")

    val r2 = rowIterator.next()
    assertEquals(row2.values.head, r2.values.head.asInstanceOf[ArrayBuffer[String]].head, "Second row content should match")

    // Trigger next batch loading
    val r3 = rowIterator.next()
    assertEquals(row3.values.head, r3.values.head.asInstanceOf[ArrayBuffer[String]].head, "Third row content should match after loading next batch")

    assertTrue(!rowIterator.hasNext, "Iterator should be exhausted")

    // Expect NoSuchElementException
    assertThrows(
      classOf[NoSuchElementException],
      () => rowIterator.next()
    )

    mockStream.close()
  }

  @Test
  def testSendDataFrame_BasicFlow(): Unit = {
    val schema = StructType(List(Column("name", StringType), Column("age", IntType)))
    val data = List(Row("Alice", 30), Row("Bob", 25))
    val df = DefaultDataFrame(schema, data.toIterator)

    val listener = new MockStreamListener()

    ServerUtils.sendDataFrame(df, listener, ServerUtilsTest.allocator)

    assertTrue(listener.onNextCalled, "Listener.onNext should be called")
    assertTrue(listener.onCompletedCalled, "Listener.onCompleted should be called")
    assertTrue(listener.lastResult.length > 0, "Result body should contain encoded Arrow data")
  }

  // --- Local Mocks ---

  class MockFlightStream(data: Seq[Row], allocator: BufferAllocator) extends FlightStream(allocator, 0, new Cancellable() {
    override def cancel(s: String, throwable: Throwable): Unit = {}
  }, new Requestor() {
    override def request(i: Int): Unit = {}
  }) {
    private val batches: Iterator[Seq[Row]] = data.grouped(2) // Batch size 2
    private var currentBatch: Seq[Row] = _
    private val testSchema = new Schema(List(Field.nullable("c1", Types.MinorType.VARCHAR.getType)).asJava)
    private val root = VectorSchemaRoot.create(testSchema, allocator)
    private val vector = root.getVector("c1").asInstanceOf[VarCharVector]

    override def next(): Boolean = {
      if (batches.hasNext) {
        currentBatch = batches.next()
        root.allocateNew()
        currentBatch.zipWithIndex.foreach { case (row, i) =>
          val value = row.get(0).toString.getBytes(StandardCharsets.UTF_8)
          vector.setSafe(i, value)
        }
        root.setRowCount(currentBatch.size)
        true
      } else {
        root.setRowCount(0)
        false
      }
    }

    override def getRoot: VectorSchemaRoot = root
    override def close(): Unit = root.close()
  }

  class MockStreamListener extends FlightProducer.StreamListener[Result] {
    var onNextCalled = false
    var onCompletedCalled = false
    var lastResult: Array[Byte] = _

    override def onNext(result: Result): Unit = {
      onNextCalled = true
      lastResult = result.getBody.array
    }

    override def onCompleted(): Unit = {
      onCompletedCalled = true
    }

    override def onError(t: Throwable): Unit = {}
  }
}