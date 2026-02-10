package link.rdcn.dacp.cook

import link.rdcn.Logging
import link.rdcn.client.{DftpClient, UrlValidator}
import link.rdcn.dacp.cook.JobStatus.{COMPLETE, FAILED, RUNNING}
import link.rdcn.dacp.optree._
import link.rdcn.dacp.recipe.ExecutionResult
import link.rdcn.operation.TransformOp
import link.rdcn.server._
import link.rdcn.server.module._
import link.rdcn.struct.{Blob, DataFrame, DataFrameMetaData, DefaultDataFrame, StreamState, Row, StructType}
import link.rdcn.user.{Credentials, UserPrincipal}
import link.rdcn.util.DataUtils
import org.json.JSONObject

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 10:59
 * @Modified By:
 */
object CookActionMethodType {
  final val SUBMIT_FLOW = "SUBMIT_FLOW"
  final val SUBMIT_RECIPE = "SUBMIT_RECIPE"
  final val GET_JOB_STATUS = "GET_JOB_STATUS"
  final val GET_JOB_EXECUTE_RESULT = "GET_JOB_EXECUTE_RESULT"
  final val GET_JOB_EXECUTE_PROCESS = "GET_JOB_EXECUTE_PROCESS"

  private final val ALL: Set[String] = Set(
    SUBMIT_FLOW,
    SUBMIT_RECIPE,
    GET_JOB_STATUS,
    GET_JOB_EXECUTE_RESULT,
    GET_JOB_EXECUTE_PROCESS
  )

  def exists(action: String): Boolean =
    ALL.contains(action)
}

class DacpCookModule extends DftpModule with Logging {

  private implicit var serverContext: ServerContext = _
  private val getMethods = new FilteredGetStreamMethods

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  private val recipeCounter = new AtomicLong(0L)
  private val jobResultCache = TrieMap.empty[String, ExecutionResult]
  private val jobTransformOpsCache = TrieMap.empty[String, Seq[TransformOp]]

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    def flowExecutionContext(userPrincipal: UserPrincipal, response: DftpResponse, jobId: String): FlowExecutionContext = new FlowExecutionContext {

      override def getJobId(): String = jobId

      override def fairdHome: String = serverContext.getDftpHome().getOrElse("./")

      override def pythonHome: String = sys.env
        .getOrElse("PYTHON_HOME", throw new Exception("PYTHON_HOME environment variable is not set"))

      override def isAsyncEnabled(wrapper: TransformFunctionWrapper): Boolean = wrapper match {
        case r: RepositoryOperator => r.transformFunctionWrapper match {
          case _: FifoFileRepositoryBundle => true
          case _  => false
        }
        case _ => false
      }

      override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
        var result: Option[DataFrame] = None
        val dftpGetStreamRequest = new DftpGetStreamRequest {
          override def getRequestPath(): String = UrlValidator.extractPath(dataFrameNameUrl)

          override def getRequestURL(): String = serverContext.baseUrl + getRequestPath()

          override def getUserPrincipal(): UserPrincipal = userPrincipal
        }

        val dftpGetStreamResponse = new DftpGetStreamResponse {
          override def sendDataFrame(dataFrame: DataFrame): Unit = result =  Some(dataFrame)

          override def sendBlob(blob: Blob): Unit = result = Some(DataUtils.blobToDataFrame(blob))

          override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
        }
        getMethods.handle(dftpGetStreamRequest, dftpGetStreamResponse)
        result
      }

      //TODO Repository config
      override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("http://10.0.89.39", 8090))

      override def loadRemoteDataFrame(baseUrl: String, transformOp: TransformOp, credentials: Credentials): Option[DataFrame] = {
        val urlInfo = UrlValidator.extractBase(baseUrl)
          .getOrElse(throw new IllegalArgumentException(s"Invalid URL format $baseUrl"))
        val client = new Client(urlInfo._2, urlInfo._3)
        client.login(credentials)
        Some(client.getRemoteDataFrame(transformOp))
      }

      private class Client(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {
        def getRemoteDataFrame(transformOp: TransformOp): DataFrame = {
          val dataFrameHandle = openDataFrame(transformOp)
          getTabular(dataFrameHandle)
        }
      }
    }

    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = {
        event match {
          case r: CollectActionMethodEvent => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectActionMethodEvent =>
            r.collect(new ActionMethod {

              override def accepts(request: DftpActionRequest): Boolean = {
                CookActionMethodType.exists(request.getActionName())
              }

              override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
                val paramsJsonObject = request.getRequestParameters()
                request.getActionName() match {
                  case CookActionMethodType.SUBMIT_FLOW =>
                    val jobId = getRecipeId()
                    val transformOps: Seq[TransformOp] = TransformTree.fromFlowdJSONString(paramsJsonObject.toString)

                    val ctx = flowExecutionContext(request.getUserPrincipal(), response, jobId)
                    val dfs = transformOps.map(_.execute(ctx))

                    val executeResult = new ExecutionResult {
                      override def single(): DataFrame = dfs.head

                      override def get(name: String): DataFrame = dfs(name.toInt - 1)

                      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
                        case (dataFrame, id) => (id.toString, dataFrame)
                      }.toMap
                    }
                    val flowProgressLogger = new FlowProgressLogger(jobId, () => getFlowProcess(transformOps) * 100,
                      () => getFlowThroughput(transformOps),
                      () => getFlowJobStatus(executeResult)
                    )
                    flowLogger(jobId).info(s"flow $jobId submitted")
                    flowProgressLogger.start()

                    jobResultCache.put(jobId, executeResult)
                    jobTransformOpsCache.put(jobId, transformOps)
                    response.sendJSONObject(new JSONObject().put("jobId", jobId))
                  case CookActionMethodType.SUBMIT_RECIPE =>
                    val transformOp = TransformTree.fromJSONObject(paramsJsonObject)
                    val dataframe = transformOp.execute(flowExecutionContext(request.getUserPrincipal(), response, null))
                    val dataFrameResponse = new DataFrameResponse {
                      override def getDataFrameMetaData: DataFrameMetaData = new DataFrameMetaData {
                        override def getDataFrameSchema: StructType = dataframe.schema
                      }

                      override def getDataFrame: DataFrame = dataframe
                    }
                    response.attachStream(dataFrameResponse)
                  case CookActionMethodType.GET_JOB_STATUS =>
                    val jobId = paramsJsonObject.optString("jobId", null)
                    if (jobId == null) {
                      response.sendError(400, "empty request require jobId")
                    }
                    val executionResult = jobResultCache.get(jobId)
                    if(executionResult.nonEmpty){
                      val status = getFlowJobStatus(executionResult.get)
                      response.sendJSONObject(status.toJSON())
                    }else response.sendError(404, s"job $jobId not exist")
                  case CookActionMethodType.GET_JOB_EXECUTE_RESULT =>
                    val jobId = paramsJsonObject.optString("jobId", null)
                    if (jobId == null) {
                      response.sendError(400, "empty request require jobId")
                    }
                    val executionResult = jobResultCache.get(jobId)
                    if(executionResult.isEmpty) response.sendError(404, s"job $jobId not exist")
                    else {
                      val jo = new JSONObject()
                      executionResult.get.map().foreach(kv => {
                        val dataFrameJson = new JSONObject()
                        dataFrameJson.put("dataframeMetaData", new DataFrameMetaData {
                            override def getDataFrameSchema: StructType = kv._2.schema
                          }.toJson())
                        dataFrameJson.put("ticket", serverContext.registry(kv._2))
                        jo.put(kv._1, dataFrameJson)
                      })
                      response.sendJSONObject(jo)
                    }
                  case CookActionMethodType.GET_JOB_EXECUTE_PROCESS =>
                    val jobId = paramsJsonObject.optString("jobId", null)
                    if (jobId == null) {
                      response.sendError(400, "empty request require jobId")
                    }
                    val transformOps = jobTransformOpsCache.get(jobId)
                    val executionResult = jobResultCache.get(jobId)
                    if(transformOps.isEmpty || executionResult.isEmpty) response.sendError(404, s"job $jobId not exist")
                    else {
                      val process = if(getFlowJobStatus(executionResult.get) == COMPLETE) 1.0 else getFlowProcess(transformOps.get)
                      response.sendJSONObject(new JSONObject()
                        .put("process", process)
                        .put("throughput", getFlowThroughput(transformOps.get)))
                    }
                  case _ => response.sendError(400, "")
                }
              }
            })
          case _ =>
        }
      }
    })

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(CollectGetStreamMethodEvent(getMethods))
      }
    })

    def getFlowProcess(transformOps: Seq[TransformOp]): Double = {
      val processSeq: Seq[Double] = transformOps.map(_.executionProgress).filter(_.nonEmpty).map(_.get)
      if(processSeq.isEmpty) -1 else processSeq.sum/processSeq.length
    }

    def getFlowThroughput(transformOps: Seq[TransformOp]): Long = {
      val traffics = transformOps.map(_.calculateTraffic).filter(_.nonEmpty).map(_.get)
      if(traffics.isEmpty) -1L else Math.round(traffics.sum / traffics.length)
    }

    def getFlowJobStatus(executionResult: ExecutionResult): JobStatus = {
      val status = executionResult.map().values.toList.map(df => df.mapIterator(iter => {
        iter.currentState
      }))
      val failed = status.find(_.isFailed)
      if(failed.nonEmpty) FAILED(failed.get.asInstanceOf[StreamState.Failed].cause)
      else if(status.contains(StreamState.Running)) RUNNING
      else COMPLETE
    }
  }

  private def getRecipeId(): String = {
    val timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(formatter)
    val seq = recipeCounter.incrementAndGet()
    s"job-${timestamp}-${seq}"
  }

  private class FlowProgressLogger(
                                    jobId: String,
                                    getProgressPercent: () => Double,
                                    getThroughput: () => Long,           // rows / second
                                    getFlowJobStatus: () => JobStatus,
                                    intervalSeconds: Long = 1
                                  ) {

    private val scheduler =
      Executors.newSingleThreadScheduledExecutor { r =>
        val t = new Thread(r, s"flow-progress-logger-$jobId")
        t.setDaemon(true)
        t
      }

    @volatile private var future: ScheduledFuture[_] = _

    def start(): Unit = {
      future = scheduler.scheduleAtFixedRate(
        () => logOnce(),
        0,
        intervalSeconds,
        TimeUnit.SECONDS
      )
    }

    def stop(): Unit = {
      if (future != null) {
        future.cancel(false)
      }
      scheduler.shutdown()
    }

    private def logOnce(): Unit = {
      try {
        flowLogger(jobId).info(
          f"JobProgress | jobId=$jobId | progress=${getProgressPercent()}%.2f%% | throughput=${getThroughput()} rows/s"
        )

        try{
          val jobStatus = getFlowJobStatus()
          jobStatus match {
            case RUNNING =>
            case COMPLETE =>
              flowLogger(jobId).info(
                s"JobProgress | jobId=$jobId | progress=100% | throughput=${getThroughput()} rows/s"
              )
              flowLogger(jobId).info(s"flow $jobId completed")
              stop()
            case FAILED(e) =>
              flowLogger(jobId).error(s"flow $jobId failed")
              logger.error(e)
              stop()
          }
        }

      } catch {
        case e: Throwable =>
          logger.warn(s"Failed to fetch job metrics, jobId=$jobId", e)
          logger.error(e)
      }
    }
  }

  override def destroy(): Unit = {}
}
