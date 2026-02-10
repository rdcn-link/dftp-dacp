/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:01
 * @Modified By:
 */
package link.rdcn.dacp.recipe

import link.rdcn.dacp.optree.FileRepositoryBundle
import link.rdcn.dacp.recipe.{FifoFileBundleFlowNode, Flow, FlowNode, FlowPath, SourceNode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

// Local mocks
case class MockOtherNode(name: String) extends FlowNode

class FlowTest {

  val nodeA: FlowNode = SourceNode("/path/A")
  val nodeB: FlowNode = MockOtherNode("B")
  val nodeC: FlowNode = MockOtherNode("C")
  val nodeD: FlowNode = MockOtherNode("D")
  val nodeE: FlowNode = FifoFileBundleFlowNode(Seq.empty, Seq.empty, Seq.empty, null)

  @Test
  def testFlowPipe_HappyPath(): Unit = {
    val flow = Flow.pipe(nodeA, nodeB, nodeC)

    val expectedNodes = Map(
      "0" -> nodeA,
      "1" -> nodeB,
      "2" -> nodeC
    )
    assertEquals(expectedNodes, flow.nodes, "Flow.pipe() node map mismatch")

    val expectedEdges = Map(
      "0" -> Seq("1"),
      "1" -> Seq("2")
    )
    assertEquals(expectedEdges, flow.edges, "Flow.pipe() edge map mismatch")
  }

  @Test
  def testFlowPipe_SingleNode(): Unit = {
    val flow = Flow.pipe(nodeA)

    assertEquals(Map("0" -> nodeA), flow.nodes, "Single node flow map mismatch")
    assertTrue(flow.edges.isEmpty, "Single node flow edges should be empty")
  }

  @Test
  def testEmptyFlow_Fails(): Unit = {
    val emptyFlow = Flow(Map.empty, Map.empty)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      emptyFlow.getExecutionPaths()
      ()
    }, "Empty flow should throw exception in getExecutionPaths")

    assertTrue(ex.getMessage.contains("Cycle detected"), "Exception message should indicate cycle detection or no root")
  }

  @Test
  def testGetExecutionPaths_Linear(): Unit = {
    val flow = Flow.pipe(nodeA, nodeB, nodeC)
    val paths = flow.getExecutionPaths()

    // C(B(A))
    val expectedPath = FlowPath(nodeC, Seq(
      FlowPath(nodeB, Seq(
        FlowPath(nodeA, Seq.empty)
      ))
    ))

    assertEquals(1, paths.length, "Linear flow should have 1 execution path")
    assertEquals(expectedPath, paths.head, "Linear execution path build incorrect")
  }

  @Test
  def testGetExecutionPaths_Branching(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeB, "C" -> nodeC)
    val edges = Map("A" -> Seq("B", "C"))
    val flow = Flow(nodes, edges)
    val paths = flow.getExecutionPaths()

    val expectedPathC = FlowPath(nodeC, Seq(FlowPath(nodeA, Seq.empty)))
    val expectedPathB = FlowPath(nodeB, Seq(FlowPath(nodeA, Seq.empty)))

    assertEquals(2, paths.length, "Branching flow should have 2 execution paths")
    assertEquals(Set(expectedPathC, expectedPathB), paths.toSet, "Branching paths incorrect")
  }

  @Test
  def testGetExecutionPaths_Merging(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeE, "C" -> nodeC)
    val edges = Map("A" -> Seq("C"), "B" -> Seq("C"))
    val flow = Flow(nodes, edges)
    val paths = flow.getExecutionPaths()

    val expectedPath = FlowPath(nodeC, Seq(
      FlowPath(nodeA, Seq.empty),
      FlowPath(nodeE, Seq.empty)
    ))

    assertEquals(1, paths.length, "Merging flow should have 1 execution path")
    assertEquals(expectedPath.node, paths.head.node, "Sink node mismatch")
    assertEquals(expectedPath.children.toSet, paths.head.children.toSet, "Parent nodes mismatch")
  }

  @Test
  def testRootNodeValidation_InvalidType_Fails(): Unit = {
    val invalidRootNode = MockOtherNode("InvalidRoot")
    val nodes = Map("A" -> invalidRootNode, "B" -> nodeB)
    val edges = Map("A" -> Seq("B"))
    val flow = Flow(nodes, edges)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      flow.getExecutionPaths()
      ()
    }, "Should throw exception if root node is not SourceNode or FifoFileBundleFlowNode")

    assertTrue(ex.getMessage.contains("is not of type SourceOp"), "Message should indicate invalid root type")
  }

  @Test
  def testNodeValidation_NodeNotInMap_Fails(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeB)
    val edges = Map("A" -> Seq("C")) // C undefined
    val flow = Flow(nodes, edges)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      flow.getExecutionPaths()
      ()
    }, "Should throw exception if node is undefined")

    assertTrue(ex.getMessage.contains("node 'C' is not defined"), "Message should indicate undefined node")
  }

  @Test
  def testCycleDetection_SimpleLoop_Fails(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeB)
    val edges = Map("A" -> Seq("B"), "B" -> Seq("A"))
    val flow = Flow(nodes, edges)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      flow.getExecutionPaths()
      ()
    }, "Cyclic graph should throw exception")

    assertTrue(ex.getMessage.contains("no root nodes found"), "Message should indicate cycle/no root")
  }
}