package link.rdcn.dacp.recipe

import org.json.{JSONArray, JSONObject}
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map => MMap, Set => MSet, Queue => MQueue}

object FlowScheduler {

  // 内部数据结构
  case class NodeInfo(name: String, nodeType: String, props: Map[String, Any], location: String)

  case class Edge(from: String, to: String)

  def schedule(sourceJsonString: String): (String, String) = {
    // 1. 解析原始 JSON
    val root = new JSONObject(sourceJsonString)
    val flowJson = root.getJSONObject("flow")
    val stopsArray = flowJson.getJSONArray("stops")
    val pathsArray = flowJson.getJSONArray("paths")

    // 2. 构建基础图信息
    val rawNodes = MMap[String, JSONObject]() // id -> json
    val inputs = MMap[String, ArrayBuffer[String]]() // node_id -> parent_ids
    val outputs = MMap[String, ArrayBuffer[String]]() // node_id -> child_ids
    val allNodeIds = MSet[String]()

    for (i <- 0 until stopsArray.length()) {
      val stop = stopsArray.getJSONObject(i)
      // 修改点：使用 "id" 而不是 "name"
      val id = stop.getString("id")
      rawNodes(id) = stop
      allNodeIds += id
      if (!inputs.contains(id)) inputs(id) = ArrayBuffer()
      if (!outputs.contains(id)) outputs(id) = ArrayBuffer()
    }

    for (i <- 0 until pathsArray.length()) {
      val path = pathsArray.getJSONObject(i)
      val from = path.getString("from")
      val to = path.getString("to")
      if (allNodeIds.contains(from) && allNodeIds.contains(to)) {
        inputs(to) += from
        outputs(from) += to
      }
    }

    // 3. 确定每个节点的 Location
    val nodeLocations: Map[String, String] = determineLocations(rawNodes.toMap, inputs.toMap)

    // 4. 选举全局主节点 (Global Master)
    val locationCounts = nodeLocations.values.groupBy(identity).mapValues(_.size)
    val globalMasterUrl = if (locationCounts.nonEmpty) locationCounts.maxBy(_._2)._1 else ""

    // 5. 寻找全局输出节点 (Sinks) - 整个DAG中没有出边的节点
    val globalSinks = allNodeIds.filter(n => outputs(n).isEmpty).toSeq

    // 6. 递归构建 Flow
    val finalFlowJson = buildLayer(globalSinks, globalMasterUrl, rawNodes.toMap, inputs.toMap, nodeLocations)

    val finalRoot = new JSONObject()
    finalRoot.put("flow", finalFlowJson)
    (finalRoot.toString(2), globalMasterUrl)
  }

  /**
   * 递归构建层级 (BFS 逻辑)
   *
   * @param targetNodes     本层级需要输出的目标节点ID集合
   * @param contextLocation 当前层级所属的节点 URL (Master)
   */
  private def buildLayer(targetNodes: Seq[String],
                         contextLocation: String,
                         rawNodes: Map[String, JSONObject],
                         inputs: Map[String, ArrayBuffer[String]],
                         nodeLocations: Map[String, String]): JSONObject = {

    val stops = new JSONArray()
    val paths = new JSONArray()

    val visitedInLayer = MSet[String]()
    val queue = MQueue[String]()
    queue ++= targetNodes

    // 收集跨节点的依赖: RemoteLocation -> Set[NodeID]
    val remoteDependencies = MMap[String, MSet[String]]()

    // 收集本层有效的边 (from -> to)
    val validEdges = ArrayBuffer[Edge]()

    while (queue.nonEmpty) {
      val currId = queue.dequeue()

      if (!visitedInLayer.contains(currId)) {
        visitedInLayer += currId

        val currLoc = nodeLocations.getOrElse(currId, "")

        if (currLoc == contextLocation) {
          // Case 1: 本地节点
          // 直接添加原节点 JSON (包含了 id, properties, version 等所有信息)
          stops.put(rawNodes(currId))

          val parents = inputs.getOrElse(currId, ArrayBuffer())
          for (p <- parents) {
            val pLoc = nodeLocations.getOrElse(p, "")

            // 记录边：暂存原始边 p -> currId
            validEdges += Edge(p, currId)

            if (pLoc == contextLocation) {
              // 父节点也是本地的 -> 继续遍历
              queue.enqueue(p)
            } else {
              // 父节点是远程的 -> 记录依赖
              if (!remoteDependencies.contains(pLoc)) {
                remoteDependencies(pLoc) = MSet()
              }
              remoteDependencies(pLoc) += p
            }
          }
        } else {
          // Case 2: 目标节点本身就是远程的 (通常仅在入口处发生)
          if (!remoteDependencies.contains(currLoc)) {
            remoteDependencies(currLoc) = MSet()
          }
          remoteDependencies(currLoc) += currId
        }
      }
    }

    // 处理远程依赖 (生成 RemoteDataFrameFlowNode 和 子 Flow)
    remoteDependencies.foreach { case (remoteUrl, requiredNodeIds) =>
      // 1. 递归构建子 Flow
      val subFlow = buildLayer(requiredNodeIds.toSeq, remoteUrl, rawNodes, inputs, nodeLocations)

      // 2. 创建 Remote 节点
      val remoteNode = new JSONObject()
      // 修改点：Remote 节点的标识符也使用 "id"
      remoteNode.put("id", remoteUrl)
      remoteNode.put("type", "RemoteDataFrameFlowNode")

      //      val props = new JSONObject()
      remoteNode.put("baseUrl", remoteUrl)
      remoteNode.put("flow", subFlow)
      remoteNode.put("certificate", "")

      stops.put(remoteNode)
    }

    // 生成 Paths
    validEdges.foreach { edge =>
      val fromLoc = nodeLocations.getOrElse(edge.from, "")

      if (nodeLocations.getOrElse(edge.to, "") == contextLocation) {

        // 如果来源是远程，"from" 指向远程节点的 ID (即 URL)
        val finalFrom = if (fromLoc == contextLocation) edge.from else fromLoc
        val finalTo = edge.to

        val pathObj = new JSONObject()
        pathObj.put("from", finalFrom)
        pathObj.put("to", finalTo)
        paths.put(pathObj)
      }
    }

    val flowJson = new JSONObject()
    flowJson.put("stops", stops)
    flowJson.put("paths", paths)
    flowJson
  }

  /**
   * 确定节点位置
   */
  private def determineLocations(rawNodes: Map[String, JSONObject],
                                 inputs: Map[String, ArrayBuffer[String]]): Map[String, String] = {
    val locations = MMap[String, String]()
    val visited = MSet[String]()

    val topoOrder = ArrayBuffer[String]()

    def dfs(u: String): Unit = {
      visited += u
      inputs.getOrElse(u, Seq()).foreach { v =>
        if (!visited.contains(v)) dfs(v)
      }
      topoOrder += u
    }

    rawNodes.keys.foreach { k => if (!visited.contains(k)) dfs(k) }

    topoOrder.foreach { id =>
      val nodeJson = rawNodes(id)
      val nType = nodeJson.getString("type")

      if (nType == "SourceNode") {
        val path = nodeJson.optString("path", "")
        locations(id) = extractUrlPrefix(path)
      } else {
        val parents = inputs.getOrElse(id, Seq())
        if (parents.isEmpty) {
          locations(id) = "unknown"
        } else {
          val parentLocs = parents.flatMap(locations.get)
          if (parentLocs.distinct.size == 1) {
            locations(id) = parentLocs.head
          } else {
            // 冲突：随机选择 (取第一个)
            locations(id) = parentLocs.headOption.getOrElse("unknown")
          }
        }
      }
    }
    locations.toMap
  }

  private def extractUrlPrefix(path: String): String = {
    val pattern = "(dacp://[0-9.]+(:[0-9]+)?)".r
    pattern.findFirstIn(path).getOrElse("unknown")
  }
}
