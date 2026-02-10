package link.rdcn.client

import link.rdcn.JobFlowLogger
import link.rdcn.dacp.catalog.{CatalogActionMethodType, DatasetMetaData}
import link.rdcn.dacp.cook.{CookActionMethodType, JobStatus}
import link.rdcn.dacp.optree._
import link.rdcn.dacp.recipe._
import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.operation._
import link.rdcn.struct._
import link.rdcn.user.{AnonymousCredentials, Credentials, UsernamePassword}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.{File, StringReader}
import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaIteratorConverter, asScalaSetConverter}
import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 20:12
 * @Modified By:
 */
class DacpClient(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {

  private val dacpUrlPrefix: String = s"dacp://$host:$port"

  def listDataSetNames(): Seq[String] = {
    val result = ArrayBuffer[String]()
    get(dacpUrlPrefix + "/listDataSets").mapIterator(rows => rows.foreach(row => {
      result+=(row.getAs[String](0))
    }))
    result
  }

  def listDataFrameNames(dsName: String): Seq[String] = {
    val result = ArrayBuffer[String]()
    get(dacpUrlPrefix+ s"/dataset/$dsName/dataframes")
      .foreach(row => result.append(row.getAs[String](0)))
    result
  }

  def getDataSetMetaData(dsName: String): Model = {
    val actionResult = doAction(CatalogActionMethodType.GET_DATASET_METADATA, new JSONObject().put("dataSetName", dsName))
    getModelByString(actionResult.result)
  }

  def getDataSetInfo(dsName: String): DatasetMetaData = {
    val actionResult = doAction("GET_DATASET_INFO", new JSONObject().put("datasetId", dsName))
    DatasetMetaData.fromJSON(actionResult.getResultJson())
  }

  def getDataFrameMetaData(dfName: String): Model = {
    val actionResult = doAction(CatalogActionMethodType.GET_DATAFRAME_METADATA, new JSONObject().put("dataFrameName", dfName))
    getModelByString(actionResult.result)
  }

  private def getModelByString(rdfString: String): Model = {
    val model = ModelFactory.createDefaultModel()
    rdfString match {
      case s if s.nonEmpty =>
        val reader = new StringReader(s)
        model.read(reader, null, "JSON-LD")
      case _ =>
    }
    model
  }

  def getSchema(dataFrameName: String): StructType = {
    val actionResult = doAction(CatalogActionMethodType.GET_SCHEMA, new JSONObject().put("dataFrameName", dataFrameName))
    StructType.fromString(actionResult.result)
  }

  def getDataFrameTitle(dataFrameName: String): String = {
    val actionResult =doAction(CatalogActionMethodType.GET_DATAFRAME_INFO, new JSONObject().put("dataFrameName", dataFrameName))
    actionResult.getResultJson().getString("title")
  }

  def getDocument(dataFrameName: String): DataFrameDocument = {
    val actionResult = doAction(CatalogActionMethodType.GET_DOCUMENT, new JSONObject().put("dataFrameName", dataFrameName))
    DataFrameDocument.fromJson(actionResult.getResultJson())
  }

  def getStatistics(dataFrameName: String): DataFrameStatistics = {
    val actionResult = doAction(CatalogActionMethodType.GET_DATAFRAME_INFO, new JSONObject().put("dataFrameName",dataFrameName))
    DataFrameStatistics.fromJson(actionResult.getResultJson())
  }

  def getHostInfo: Map[String, Any] = {
    val jo = doAction(CatalogActionMethodType.GET_HOST_INFO).getResultJson()
    jo.keys().asScala.map { key =>
      key -> jo.get(key)
    }.toMap
  }

  def getServerResourceInfo: Map[String, Any] = {
    val jo = doAction(CatalogActionMethodType.GET_SERVER_INFO).getResultJson()
    jo.keys().asScala.map { key =>
      key -> jo.get(key)
    }.toMap
  }

  def executeTransformTree(transformOp: TransformOp): DataFrame = {
    val dataFrameHandle = submitRecipe(transformOp)
    getTabular(dataFrameHandle)
  }

  def cook(recipe: Flow): ExecutionResult = {
    val executePaths: Seq[FlowPath] = recipe.getExecutionPaths()
    val dfs: Seq[DataFrame] = executePaths.map(path => {
      RemoteDataFrameProxy(transformFlowToOperation(path), getStream, submitRecipe)
    })
    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt - 1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }

  private def submitRecipe(transformOp: TransformOp): DataFrameHandle = {
    val responseJson = new JSONObject(doAction(CookActionMethodType.SUBMIT_RECIPE, transformOp.toJsonString).result)

    new DataFrameHandle {
      override def getDataFrameMeta: DataFrameMetaData =
        DataFrameMetaData.fromJson(responseJson.getJSONObject("dataframeMetaData"))

      override def getDataFrameTicket: DftpTicket = responseJson.getString("ticket")
    }
  }

  def cook(flowJson: String): String = {
    val actionResult = doAction(CookActionMethodType.SUBMIT_FLOW, flowJson)
    actionResult.getResultJson().getString("jobId")
  }

  def getJobStatus(jobId: String): JobStatus = {
    val actionResult = doAction(CookActionMethodType.GET_JOB_STATUS, new JSONObject().put("jobId", jobId))
    JobStatus.fromJSON(actionResult.getResultJson())
  }

  def getJobExecuteProcess(jobId: String): String = {
    val actionResult = doAction(CookActionMethodType.GET_JOB_EXECUTE_PROCESS, new JSONObject().put("jobId", jobId))
    actionResult.result
  }

  def getJobExecuteResult(jobId: String): ExecutionResult = {
    val actionResult = doAction(CookActionMethodType.GET_JOB_EXECUTE_RESULT, new JSONObject().put("jobId", jobId)).getResultJson()
    lazy val dataFrames: Map[String, DataFrame] = actionResult.keySet().asScala.map(key => {
      val dataFrameJson = actionResult.getJSONObject(key)
      val dataFrameHandle = new DataFrameHandle {
        override def getDataFrameMeta: DataFrameMetaData =
          DataFrameMetaData.fromJson(dataFrameJson.getJSONObject("dataframeMetaData"))

        override def getDataFrameTicket: DftpTicket = dataFrameJson.getString("ticket")
      }
      (key,getTabular(dataFrameHandle))
    }).toMap
    new ExecutionResult {
      override def single(): DataFrame = dataFrames.head._2

      override def get(name: String): DataFrame =
        dataFrames.get(name).getOrElse(throw new Exception(s"$name DataFrame Not Found"))

      override def map(): Map[String, DataFrame] = dataFrames
    }
  }

  private def transformFlowToOperation(path: FlowPath): TransformOp = {
    path.node match {
      case f: Transformer11 =>
        val flowGenericFunctionCall: FlowGenericFunctionCall = DataFrameCall11(
          new SerializableFunction[(DataFrame, JobFlowLogger, JSONObject), DataFrame] {
            override def apply(v1: (DataFrame, JobFlowLogger, JSONObject)): DataFrame =
              f.transform(v1._1, v1._2, v1._3)
          })
        val transformerNode: TransformerNode = TransformerNode(
          TransformFunctionWrapper.getJavaSerialized(flowGenericFunctionCall),
          transformFlowToOperation(path.children.head))
        transformerNode
      case f: Transformer21 =>
        val genericFunctionCall = DataFrameCall21(
          new SerializableFunction[((DataFrame, DataFrame), JobFlowLogger, JSONObject), DataFrame] {
            override def apply(v1: ((DataFrame, DataFrame), JobFlowLogger, JSONObject)): DataFrame = {
              f.transform(v1._1, v1._2, v1._3)
            }
          })
        val leftInput = transformFlowToOperation(path.children.head)
        val rightInput = transformFlowToOperation(path.children.last)
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.getJavaSerialized(genericFunctionCall),
          leftInput, rightInput)
        transformerNode
      case node: RepositoryNode =>
        val transformerNode: TransformerNode = TransformerNode(
          RepositoryOperator(node.functionName, node.functionVersion, node.params, node.id),
          path.children.map(transformFlowToOperation(_)): _*)
        transformerNode
      case FifoFileBundleFlowNode(command, inputFilePath, outputFilePath, dockerContainer) =>
        val jo = new JSONObject()
        jo.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
        jo.put("command", new JSONArray(command.asJavaCollection))
        jo.put("inputFilePath", new JSONArray(inputFilePath.map(file => new JSONObject().put("filePath", file._1)
        .put("fileType", file._2)).asJavaCollection))
        jo.put("outputFilePath", new JSONArray(outputFilePath.map(file => new JSONObject().put("filePath", file._1)
          .put("fileType", file._2)).asJavaCollection))
        jo.put("dockerContainer", dockerContainer.toJson())
        val transformerNode: TransformerNode = TransformerNode(
          TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[FileRepositoryBundle],
          path.children.map(transformFlowToOperation(_)): _* )
        transformerNode
      case FifoFileFlowNode() => FiFoFileNode(path.children.map(transformFlowToOperation(_)): _*)
      case RemoteDataFrameFlowNode(baseUrl, flow, certificate) =>
        RemoteSourceProxyOp(baseUrl, transformFlowToOperation(flow.getExecutionPaths().head), certificate)
      case s: SourceNode => SourceOp(s.dataFrameName)
      case other => throw new IllegalArgumentException(s"This FlowNode ${other} is not supported please extend Transformer11 trait")
    }
  }
}

object DacpClient {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = null): DacpClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101))
        if(credentials != null) client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, file: File, credentials: Credentials = null): DacpClient = {
    System.setProperty("javax.net.ssl.trustStore", file.getAbsolutePath)
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101), true)
        if(credentials != null) client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
