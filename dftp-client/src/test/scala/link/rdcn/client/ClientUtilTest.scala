/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 11:08
 * @Modified By:
 */
package link.rdcn.client

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 11:08
 * @Modified By:
 */
import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import org.apache.arrow.flight.{PutResult, Result, SyncPutListener}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.types.{FloatingPointPrecision, Types}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api._

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.collection.JavaConverters._

class ClientUtilsJunitTest {
  private val allocator = new RootAllocator(Long.MaxValue)

  @Test
  def testArrowSchemaToStructType_PrimitiveTypes(): Unit = {
    // 覆盖 Int, Long, Float, Double, Boolean 的 case
    val arrowFields = List(
      new Field("c1", new FieldType(true, Types.MinorType.INT.getType, null), Collections.emptyList()),
      new Field("c2", new FieldType(true, Types.MinorType.BIGINT.getType, null), Collections.emptyList()),
      new Field("c3", new FieldType(true, Types.MinorType.FLOAT4.getType, null), Collections.emptyList()),
      new Field("c4", new FieldType(true, Types.MinorType.FLOAT8.getType, null), Collections.emptyList()),
      new Field("c5", new FieldType(true, Types.MinorType.BIT.getType, null), Collections.emptyList())
    ).asJava

    val arrowSchema = new Schema(arrowFields)
    val structType = ClientUtils.arrowSchemaToStructType(arrowSchema)

    val expectedColumns = List(
      Column("c1", IntType),
      Column("c2", LongType),
      Column("c3", FloatType),
      Column("c4", DoubleType),
      Column("c5", BooleanType)
    )

    assertEquals(StructType(expectedColumns), structType)
  }

  @Test
  def testArrowSchemaToStructTypeStringAndBinaryTypes(): Unit = {
    // 覆盖 VARCHAR, VARBINARY 的普通类型和带元数据的特殊类型 (RefType, BinaryType)
    val refMetadata = Map("logicalType" -> "Url").asJava
    val blobMetadata = Map("logicalType" -> "blob").asJava

    val arrowFields = List(
      // 普通 StringType (VARCHAR, 无元数据)
      new Field("c1", new FieldType(true, Types.MinorType.VARCHAR.getType, null), Collections.emptyList()),
      // RefType (VARCHAR, 有元数据)
      new Field("c2", new FieldType(true, Types.MinorType.VARCHAR.getType, null, refMetadata), Collections.emptyList()),
      // 普通 BinaryType (VARBINARY, 无元数据)
      new Field("c3", new FieldType(true, Types.MinorType.VARBINARY.getType, null), Collections.emptyList()),
      // BinaryType (VARBINARY, 有元数据)
      new Field("c4", new FieldType(true, Types.MinorType.VARBINARY.getType, null, blobMetadata), Collections.emptyList())
    ).asJava

    val arrowSchema = new Schema(arrowFields)
    val structType = ClientUtils.arrowSchemaToStructType(arrowSchema)

    val expectedColumns = List(
      Column("c1", StringType),
      Column("c2", RefType),
      Column("c3", BinaryType),
      Column("c4", BinaryType)
    )

    assertEquals(StructType(expectedColumns), structType)
  }

  @Test()
  def testArrowSchemaToStructTypeUnsupportedType(): Unit = {
    // 覆盖 default case
    val unsupportedType = Types.MinorType.DATEDAY.getType // 使用一个未处理的 Arrow Type
    val arrowFields = List(
      new Field("c1", new FieldType(true, unsupportedType, null), Collections.emptyList())
    ).asJava
    val arrowSchema = new Schema(arrowFields)
    val exception = assertThrows(
      classOf[UnsupportedOperationException], () => ClientUtils.arrowSchemaToStructType(arrowSchema))
    assertEquals(exception.getMessage, s"Unsupported Arrow type: ${unsupportedType}")

  }

  @Test
  def testGetVectorSchemaRootFromBytes(): Unit = {
    val field = Field.nullable("data", Types.MinorType.INT.getType)
    val schema = new Schema(Collections.singletonList(field))
    val root = VectorSchemaRoot.create(schema, allocator)

    val intVector = root.getVector("data").asInstanceOf[IntVector]
    intVector.allocateNew()
    intVector.setSafe(0, 123)
    root.setRowCount(1)

    val outputStream = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, outputStream)
    writer.writeBatch()
    writer.end()

    val bytes = outputStream.toByteArray
    outputStream.close()

    val newRoot = ClientUtils.getVectorSchemaRootFromBytes(bytes, allocator)

    assertTrue(newRoot.getRowCount > 0, "Should load the next batch")
    assertEquals(schema, newRoot.getSchema, "Schema should match")

    newRoot.close()
    root.close()
    writer.close()
  }

  @Test
  def testConvertStructTypeToArrowSchemaAllTypes(): Unit = {
    // 覆盖所有的 ValueType case: Int, Long, Float, Double, String, Boolean, Binary, Ref, Blob
    val structType = StructType(List(
      Column("c1", IntType, nullable = false), // IntType, 32 bit, non-nullable
      Column("c2", LongType), // LongType, 64 bit
      Column("c3", FloatType), // FloatType, single precision
      Column("c4", DoubleType), // DoubleType, double precision
      Column("c5", StringType), // StringType, Utf8
      Column("c6", BooleanType), // BooleanType, Bool
      Column("c7", BinaryType), // BinaryType, Binary
      Column("c8", RefType), // RefType, Utf8 with metadata
      Column("c9", BinaryType) // BinaryType, Binary with metadata
    ))

    val arrowSchema = ClientUtils.convertStructTypeToArrowSchema(structType)
    val fields = arrowSchema.getFields.asScala.toList

    assertEquals(Types.MinorType.INT.getType, fields(0).getType)
    assertTrue(!fields(0).isNullable)

    assertEquals(Types.MinorType.BIGINT.getType, fields(1).getType)

    assertTrue(fields(2).getType.isInstanceOf[ArrowType.FloatingPoint])
    assertEquals(FloatingPointPrecision.SINGLE, fields(2).getType.asInstanceOf[ArrowType.FloatingPoint].getPrecision)

    assertTrue(fields(3).getType.isInstanceOf[ArrowType.FloatingPoint])
    assertEquals(FloatingPointPrecision.DOUBLE, fields(3).getType.asInstanceOf[ArrowType.FloatingPoint].getPrecision)

    assertEquals(ArrowType.Utf8.INSTANCE, fields(4).getType)
    assertTrue(fields(4).getMetadata.isEmpty)

    assertEquals(ArrowType.Bool.INSTANCE, fields(5).getType)

    assertEquals(new ArrowType.Binary(), fields(6).getType)
    assertTrue(fields(6).getMetadata.isEmpty)

    assertEquals(ArrowType.Utf8.INSTANCE, fields(7).getType)
    assertEquals("Url", fields(7).getMetadata.get("logicalType"))

    assertEquals(new ArrowType.Binary(), fields(8).getType)
    assertEquals("blob", fields(8).getMetadata.get("logicalType"))
  }

  @Test
  def testParsePutListenerNoMetadata(): Unit = {
    val putListener = new SyncPutListener() // 实例化一个真实的类，不向其写入任何内容
    val queueField = classOf[SyncPutListener].getDeclaredField("queue")
    queueField.setAccessible(true)
    val queue = queueField.get(putListener).asInstanceOf[java.util.concurrent.BlockingQueue[PutResult]]
    queue.clear() // 确保队列为空

    // 修改处：类型改为 Option[Iterator[String]] 以匹配 ClientUtils.parsePutListener 的返回类型
    var result: Option[Iterator[String]] = None

    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        // 阻塞操作在单独线程中执行
        // 修改处：将结果包裹在 Some 中
        result = Some(ClientUtils.parsePutListener(putListener))
      }
    })
    thread.start()
    Thread.sleep(100)
    thread.interrupt()
    thread.join(500)
    assertTrue(!thread.isAlive, "Test thread should have stopped after interrupt")
    assertTrue(result.isEmpty, "Result should be None or handle the interruption gracefully")
  }
}