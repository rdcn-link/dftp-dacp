/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:05
 * @Modified By:
 */
package link.rdcn.dacp.optree

import link.rdcn.dacp.optree._
import link.rdcn.dacp.optree.fifo.RowFilePipe
import link.rdcn.operation._
import link.rdcn.struct.ValueType.IntType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.Credentials
import org.json.JSONObject
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

class TransformTreeTest {

  // --- Local Mocks ---

  class MockTransformerNode(wrapper: TransformFunctionWrapper, input: TransformOp) extends TransformerNode(wrapper, input) {
    val released = new AtomicBoolean(false)
    inputs = Seq(input)

    override def release(): Unit = {
      super.release()
      released.set(true)
    }
  }

  class MockReleasableTransformerNode extends TransformerNode(null) {
    val released = new AtomicBoolean(false)

    override def release(): Unit = released.set(true)
  }

  class MockTransformFunctionWrapper(name: String, dfToReturn: DataFrame) extends TransformFunctionWrapper {
    var applyCalledWith: Seq[DataFrame] = _

    override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
      applyCalledWith = inputs
      dfToReturn
    }

    override def toJson: JSONObject = new JSONObject()
  }

  class MockFlowExecutionContext(async: Boolean) extends FlowExecutionContext {
    val registeredFutures = scala.collection.mutable.ArrayBuffer[TransformOp]()

    override def isAsyncEnabled(wrapper: TransformFunctionWrapper): Boolean = async

    override def registerAsyncResult(transformOp: TransformOp, future: Future[DataFrame], thread: Thread): Unit = {
      registeredFutures.append(transformOp)
    }

    // Stubs
    override def fairdHome: String = ""

    override def pythonHome: String = ""

    override def loadRemoteDataFrame(baseUrl: String, path: TransformOp, credentials: Credentials): Option[DataFrame] = None

    override def getRepositoryClient(): Option[OperatorRepository] = None

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None

    override def getJobId(): String = ""
  }

  // --- Tests ---

  @Test
  def testFromJsonString_SourceOp(): Unit = {
    val json = """{"type": "SourceOp", "dataFrameName": "/my/data"}"""
    val op = TransformTree.fromJsonString(json)
    assertNotNull(op, "Parsed Op should not be null")
    assertTrue(op.isInstanceOf[SourceOp], "Op type should be SourceOp")
    assertEquals("/my/data", op.asInstanceOf[SourceOp].dataFrameUrl, "dataFrameName mismatch")
  }

  @Test
  def testTransformerNode_Release(): Unit = {
    val child = new MockReleasableTransformerNode()
    val parent = new MockTransformerNode(new MockTransformFunctionWrapper("parent", null), child)
    val root = new MockTransformerNode(new MockTransformFunctionWrapper("root", null), parent)

    root.release()

    assertTrue(child.released.get(), "Child node release() should be called recursively")
  }

  @Test
  def testTransformerNode_Execute_Sync(): Unit = {
    val mockCtx = new MockFlowExecutionContext(async = false)
    val inputDf = DefaultDataFrame(StructType.empty.add("in", IntType), Seq(Row(1)).iterator)
    val expectedDf = DefaultDataFrame(StructType.empty.add("out", IntType), Seq(Row(2)).iterator)

    val inputOp = new TransformOp {
      override def execute(ctx: ExecutionContext): DataFrame = inputDf

      override def operationType: String = "Mock"

      override def toJson: JSONObject = new JSONObject()

      override var inputs: Seq[TransformOp] = Seq.empty
    }

    val mockFunc = new MockTransformFunctionWrapper("func", expectedDf)
    val node = TransformerNode(mockFunc, inputOp)

    val result = node.execute(mockCtx)

    assertEquals(expectedDf, result, "Returned DataFrame mismatch")
    assertEquals(Seq(inputDf), mockFunc.applyCalledWith, "Function received incorrect input")
  }

  @Test
  def testFiFoFileNode_Execute(@TempDir tempDir: File): Unit = {
    val pipeFile = new File(tempDir, "test.pipe")
    val expectedData = Seq("hello", "pipe")

    val inputOp = new TransformOp {
      override def execute(ctx: ExecutionContext): DataFrame = {
        val pipe = new RowFilePipe(pipeFile)
        pipe.write(expectedData.iterator)
        DataFrame.fromSeq(expectedData)
      }

      override def operationType: String = "MockWriter"

      override def toJson: JSONObject = new JSONObject()

      override var inputs: Seq[TransformOp] = Seq.empty
    }

    val node = FiFoFileNode(inputOp)
    val mockCtx = new MockFlowExecutionContext(async = false)

    val resultDf = node.execute(mockCtx)
    val resultData = resultDf.collect().map(_.getAs[String](0))

    assertEquals(expectedData, resultData, "FiFoFileNode failed to read from pipe")
  }
}