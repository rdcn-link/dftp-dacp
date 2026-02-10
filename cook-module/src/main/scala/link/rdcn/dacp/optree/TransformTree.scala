package link.rdcn.dacp.optree

import link.rdcn.Logging
import link.rdcn.dacp.optree.fifo.FileType
import link.rdcn.operation._
import link.rdcn.struct.DataFrame
import link.rdcn.user.TokenAuth
import org.json.{JSONArray, JSONObject}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.mutable.{Map => MMap}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/18 10:53
 * @Modified By:
 */

/**
 * Tree-structured operation collection with hierarchical execution
 * Example:
 * op1 op2 op3
 *  \   /  /
 *   op4 op5
 *     \ /
 *     op6
 */
object TransformTree {

  def fromFlowdJSONString(flowString: String): Seq[TransformOp] = {
    val rootJson = new JSONObject(flowString)
    val flowJson = if (rootJson.has("flow")) rootJson.getJSONObject("flow") else rootJson

    val stopsArray = flowJson.getJSONArray("stops")
    val pathsArray = flowJson.getJSONArray("paths")

    val nodesMap = MMap[String, JSONObject]()
    for (i <- 0 until stopsArray.length()) {
      val stop = stopsArray.getJSONObject(i)
      nodesMap(stop.getString("id")) = stop
    }

    val incomingEdges = MMap[String, mutable.Buffer[(String, Int)]]()
    val allSourceIds = mutable.Set[String]()

    for (i <- 0 until pathsArray.length()) {
      val path = pathsArray.getJSONObject(i)
      val from = path.getString("from")
      val to = path.getString("to")

      val inportStr = path.optString("toPort", "0")
      val inport = try {
        inportStr.toInt
      } catch {
        case _: NumberFormatException => 0
      }

      incomingEdges.getOrElseUpdate(to, mutable.Buffer()) += ((from, inport))
      allSourceIds.add(from)
    }

    val allIds = nodesMap.keys.toSet
    val sinkIds = allIds -- allSourceIds

    if (sinkIds.isEmpty) throw new IllegalArgumentException("Invalid Flow: Cyclic graph or empty (No sink node found).")

    def recursiveBuild(currentId: String): TransformOp = {
      val nodeJson = nodesMap(currentId)
      val nodeType = nodeJson.getString("type")

      val edges = incomingEdges.getOrElse(currentId, mutable.Buffer.empty)

      val sortedInputIds = edges.sortBy(_._2).map(_._1)

      val inputOps = sortedInputIds.map(recursiveBuild).toSeq

      nodeType match {
        case "SourceNode" =>
          val path = if (nodeJson.has("dataFrameName")) nodeJson.getString("dataFrameName")
          else nodeJson.optString("path", "")
          SourceOp(path)

        case "RepositoryNode" =>
          val jo = new JSONObject()
            .put("type", LangTypeV2.REPOSITORY_OPERATOR.name)
            .put("functionName", nodeJson.get("name").asInstanceOf[String])
            .put("functionVersion", nodeJson.get("version").asInstanceOf[String])
            .put("params", nodeJson.getJSONObject("parameters"))
            .put("id", nodeJson.getString("id"))


          TransformerNode(
            TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[RepositoryOperator],
            inputOps: _*
          )

        case "RemoteDataFrameFlowNode" =>
          RemoteSourceProxyOp(
            nodeJson.get("baseUrl").asInstanceOf[String],
            fromFlowdJSONString(new JSONObject().put("flow", nodeJson.getJSONObject("flow")).toString).head,
            nodeJson.get("certificate").asInstanceOf[String]
          )

        case other => throw new IllegalArgumentException(s"Unknown FlowNode type: $other at id: $currentId")
      }
    }

    sinkIds.map(recursiveBuild(_)).toSeq
  }

  def fromJSONObject(parsed: JSONObject): TransformOp = {
    val opType = parsed.getString("type")
    if (opType == "SourceOp") {
      SourceOp(parsed.getString("dataFrameName"))
    } else if (opType == "RemoteSourceProxyOp") {
      RemoteSourceProxyOp(parsed.getString("baseUrl"), fromJSONObject(parsed.getJSONObject("transformOp")), parsed.getString("token"))
    } else {
      val ja: JSONArray = parsed.getJSONArray("input")
      val inputs = (0 until ja.length).map(ja.getJSONObject(_)).map(fromJSONObject(_))
      opType match {
        case "Map" => MapOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Filter" => FilterOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Limit" => LimitOp(parsed.getJSONArray("args").getInt(0), inputs: _*)
        case "Select" => SelectOp(inputs.head, parsed.getJSONArray("args").toList.asScala.map(_.toString): _*)
        case "TransformerNode" => TransformerNode(TransformFunctionWrapper.fromJsonObject(parsed.getJSONObject("function")), inputs: _*)
        case "FifoFileNode" => FiFoFileNode(inputs: _*)
      }
    }
  }

  def fromJsonString(json: String): TransformOp = fromJSONObject(new JSONObject(json))
}

case class RemoteSourceProxyOp(baseUrl: String, transformOp: TransformOp, certificate: String) extends TransformOp {

  override var inputs: Seq[TransformOp] = Seq.empty

  override def sourceUrlList: Set[String] = Set.empty

  override def operationType: String = "RemoteSourceProxyOp"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("baseUrl", baseUrl).put("transformOp", transformOp.toJson).put("token", certificate)

  override def execute(ctx: ExecutionContext): DataFrame = {
    require(ctx.isInstanceOf[FlowExecutionContext])
    ctx.asInstanceOf[FlowExecutionContext].loadRemoteDataFrame(baseUrl, transformOp, TokenAuth(certificate))
      .getOrElse(throw new Exception(s"get remote Source ${transformOp.toJsonString} fail"))
  }
}

case class FiFoFileNode(transformOp: TransformOp*) extends TransformOp
{
  override var inputs: Seq[TransformOp] = transformOp

  override def operationType: String = "FifoFileNode"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(in => ja.put(in.toJson))
    new JSONObject().put("type", operationType)
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val df = transformOp.head.execute(ctx)
    df
  }
}

case class TransformerNode(transformFunctionWrapperT: TransformFunctionWrapper, inputTransforms: TransformOp*)
  extends TransformOp with Logging {

  var transformFunctionWrapper: TransformFunctionWrapper = _

  def contain(transformerNode: TransformerNode): Boolean = {
    (transformerNode eq this) || inputTransforms.exists(op => {
      if(op.isInstanceOf[TransformerNode])
        op.asInstanceOf[TransformerNode].contain(transformerNode)
      else false
    })
  }

  def release(): Unit = {
    if(transformFunctionWrapper.isInstanceOf[FileRepositoryBundle]){
      transformFunctionWrapper.asInstanceOf[FileRepositoryBundle]
        .deleteFile
    }
    inputTransforms.foreach(input => {
      if(input.isInstanceOf[TransformerNode]){
        input.asInstanceOf[TransformerNode].release()
      }
    })
  }

  override var inputs: Seq[TransformOp] = inputTransforms

  override def operationType: String = "TransformerNode"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(in => ja.put(in.toJson))
    new JSONObject().put("type", operationType)
      .put("function", transformFunctionWrapperT.toJson)
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val flowCtx = ctx.asInstanceOf[FlowExecutionContext]
    val inputDataFrames = inputs.map(_.execute(ctx))
    val result = transformFunctionWrapperT.applyToDataFrames(inputDataFrames, flowCtx)
    transformFunctionWrapper = transformFunctionWrapperT match {
      case r: RepositoryOperator => r.transformFunctionWrapper
      case _ => transformFunctionWrapperT
    }
    transformFunctionWrapper match {
      case bundle: FileRepositoryBundle if bundle.outputFilePath.head._2 == FileType.FIFO_BUFFER =>
          var thread: Thread = null
          val future: Future[DataFrame] = Future {
            try {
              thread = Thread.currentThread()
              bundle.runOperator(result)
            } catch {
              case t: Throwable =>
                t.printStackTrace()
                DataFrame.empty()
            }
          }
          flowCtx.registerAsyncResult(this, future, thread)
      case bundle: FileRepositoryBundle if bundle.outputFilePath.head._2 == FileType.MMAP_FILE =>
        try {
          bundle.runOperator(result)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          try {
            this.asInstanceOf[TransformerNode].release()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      case _ =>
    }
    result
  }
}
