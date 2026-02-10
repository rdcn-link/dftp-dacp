package link.rdcn.server

import link.rdcn.Logging
import link.rdcn.client.UrlValidator
import link.rdcn.message.DftpTicket
import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.server.ServerUtils.convertStructTypeToArrowSchema
import link.rdcn.server.exception.{TicketExpiryException, TicketNotFoundException}
import link.rdcn.server.module.KernelModule
import link.rdcn.struct._
import link.rdcn.user.UserPrincipal
import link.rdcn.util.{CodecUtils, DataUtils}
import org.apache.arrow.flight._
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.json.JSONObject
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.io.FileUrlResource

import java.io.{File, InputStream}
import java.nio.charset.StandardCharsets
import java.security.{PrivateKey, PublicKey}
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport
import java.util.{Optional, UUID}
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:31
 * @Modified By:
 */

trait ServerContext {
  def getHost(): String
  def getPort(): Int
  def getProtocolScheme(): String
  def getDftpHome(): Option[String]
  def getPublicKeyMap(): Map[String, PublicKey] = Map.empty
  def getPrivateKey: Option[PrivateKey] = None
  def registry(dataframe: DataFrame): DftpTicket
  def registry(blob: Blob): DftpTicket
  def baseUrl: String = s"${getProtocolScheme()}://${getHost()}:${getPort()}"
}

class DftpServer(config: DftpServerConfig) extends Logging {

  private val location = if (config.useTls) {
    Location.forGrpcTls(config.host, config.port)
  } else {
    Location.forGrpcInsecure(config.host, config.port)
  }
  private val uriPool = new URIReferencePool

  private val kernelModule = new KernelModule()
  protected val modules = new Modules(new ServerContext() {

    override def getHost(): String = config.host

    override def getPort(): Int = config.port

    override def getProtocolScheme(): String = config.protocolScheme

    override def getDftpHome(): Option[String] = config.dftpHome

    override def getPublicKeyMap(): Map[String, PublicKey] = config.pubKeyMap

    override def getPrivateKey: Option[PrivateKey] = config.privateKey

    override def registry(dataframe: DataFrame): DftpTicket = uriPool.registryDataFrame(dataframe)

    override def registry(blob: Blob): DftpTicket = uriPool.registryBlob(blob)
  })

  private val authenticatedUserMap = new ConcurrentHashMap[String, UserPrincipal]()

  @volatile private var allocator: BufferAllocator = _
  @volatile private var flightServer: FlightServer = _
  @volatile private var serverThread: Thread = _
  @volatile private var started: Boolean = false

  private val putDataFrameParametersCache = TrieMap[String, JSONObject]()
  private val putBlobParametersCache = TrieMap[String, JSONObject]()

  def startBlocking(): Unit = synchronized {
    if (!started) {
      buildServer()
      try {
        flightServer.start()
        started = true

        Runtime.getRuntime.addShutdownHook(new Thread(() => close()))

        flightServer.awaitTermination()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      } finally {
        started = false
      }
    }
  }

  def start(): Unit = synchronized {
    if (!started) {
      buildServer()
      serverThread = new Thread(() => {
        try {
          flightServer.start()
          started = true
          Runtime.getRuntime.addShutdownHook(new Thread(() => {
            close()
          }))
          flightServer.awaitTermination()
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          started = false
        }
      })

      serverThread.setDaemon(false)
      serverThread.start()
    }
  }

  def close(): Unit = synchronized {
    if (!started) return
    try {
      if (flightServer != null) flightServer.close()
    } catch {
      case _: Throwable => // ignore
    }

    try {
      if (allocator != null) allocator.close()
    } catch {
      case _: Throwable => // ignore
    }

    if (serverThread != null && serverThread.isAlive) {
      serverThread.interrupt()
    }

    // reset
    flightServer = null
    allocator = null
    serverThread = null
    started = false
  }

  private def buildServer(): Unit = {
    allocator = new RootAllocator()
    val producer = new DftpFlightProducer()

    if (config.useTls) {
      flightServer = FlightServer.builder(allocator, location, producer)
        .useTls(config.tlsCertFile.get, config.tlsKeyFile.get)
        .authHandler(new FlightServerAuthHandler)
        .build()
    } else {
      flightServer = FlightServer.builder(allocator, location, producer)
        .authHandler(new FlightServerAuthHandler)
        .build()
    }
    logger.info(s"server started at ${config.protocolScheme}://${config.host}:${config.port}")
    modules.addModule(kernelModule)
    modules.init()
  }

  private class FlightServerAuthHandler extends ServerAuthHandler {
    override def authenticate(serverAuthSender: ServerAuthHandler.ServerAuthSender, iterator: util.Iterator[Array[Byte]]): Boolean = {
      try {
        val cred = CodecUtils.decodeCredentials(iterator.next())
        val authenticatedUser = kernelModule.authenticate(cred)
        val token = UUID.randomUUID().toString()
        authenticatedUserMap.put(token, authenticatedUser)
        serverAuthSender.send(CodecUtils.encodeString(token))
        true
      } catch {
        case e: Exception => false
      }
    }

    override def isValid(bytes: Array[Byte]): Optional[String] = {
      val tokenStr = CodecUtils.decodeString(bytes)
      Optional.of(tokenStr)
    }
  }

  private class DftpFlightProducer extends NoOpFlightProducer with Logging {

    override def doAction(callContext: FlightProducer.CallContext,
                          action: Action,
                          listener: FlightProducer.StreamListener[Result]): Unit = {

      val actionResponse = new DftpActionResponse {

        override def attachStream(dataframeResponse: DataFrameResponse): Unit = {
          val responseJsonObject = new JSONObject()
          val dataframe = dataframeResponse.getDataFrame
          val dftpTicket: DftpTicket = uriPool.registryDataFrame(dataframe)
          responseJsonObject
            .put("dataframeMetaData", dataframeResponse.getDataFrameMetaData.toJson())
            .put("ticket", dftpTicket)
          sendJSONObject(responseJsonObject, 304)
        }

        override def attachStream(blobResponse: BlobResponse): Unit = {
          val dftpTicket: DftpTicket = uriPool.registryBlob(blobResponse.getBlob)
          sendJSONObject(new JSONObject()
            .put("ticket", dftpTicket)
            .put("size", blobResponse.size)
            .put("blobType", blobResponse.blobType), 304)
        }

        override def sendError(errorCode: Int, message: String): Unit = {
          sendErrorWithFlightStatus(errorCode, message)
        }

        override def sendJSONString(json: String, code: Int = 200): Unit = {
          listener.onNext(new Result(CodecUtils.encodeString(code.toString)))
          listener.onNext(new Result(CodecUtils.encodeString(json)))
          listener.onCompleted()
        }

        override def sendPutDataFrameParameters(json: JSONObject, code: Int): Unit = {
          val putTicket = UUID.randomUUID().toString
          putDataFrameParametersCache.put(putTicket, json)
        }

        override def sendPutBlobParameters(json: JSONObject, code: Int): Unit = {
          val putTicket = UUID.randomUUID().toString
          putBlobParametersCache.put(putTicket, json)
        }
      }

      val actionRequest = new DftpActionRequest {

        override def getActionName(): String = action.getType

        lazy val requestParameters: JSONObject = {
          new JSONObject(CodecUtils.decodeString(action.getBody))
        }

        override def getUserPrincipal(): UserPrincipal =
          authenticatedUserMap.get(callContext.peerIdentity())

        override def getRequestParameters(): JSONObject = requestParameters
      }

      kernelModule.doAction(actionRequest, actionResponse)
    }

    /**
     * 400 Bad Request → 请求参数非法，对应 INVALID_ARGUMENT
     * 401 Unauthorized → 未认证，对应 UNAUTHENTICATED
     * 403 Forbidden → 没有权限，Flight 没有 PERMISSION_DENIED，用 UNAUTHORIZED 替代
     * 404 Not Found → 资源未找到，对应 NOT_FOUND
     * 408 Request Timeout → 请求超时，对应 TIMED_OUT
     * 409 Conflict → 冲突，比如资源已存在，对应 ALREADY_EXISTS
     * 500 Internal Server Error → 服务端内部错误，对应 INTERNAL
     * 501 Not Implemented → 未实现的功能，对应 UNIMPLEMENTED
     * 503 Service Unavailable → 服务不可用（可能是过载或维护），对应 UNAVAILABLE
     * 其它未知错误 → 映射为 UNKNOWN
     * */
    private def sendErrorWithFlightStatus(code: Int, message: String): Unit = {
      val status = code match {
        case 400 => CallStatus.INVALID_ARGUMENT
        case 401 => CallStatus.UNAUTHENTICATED
        case 403 => CallStatus.UNAUTHORIZED
        case 404 => CallStatus.NOT_FOUND
        case 408 => CallStatus.TIMED_OUT
        case 409 => CallStatus.ALREADY_EXISTS
        case 500 => CallStatus.INTERNAL
        case 501 => CallStatus.UNIMPLEMENTED
        case 503 => CallStatus.UNAVAILABLE
        case _ => CallStatus.UNKNOWN
      }
      throw status.withDescription(message).toRuntimeException
    }

    override def getStream(callContext: FlightProducer.CallContext,
                           ticket: Ticket,
                           listener: FlightProducer.ServerStreamListener): Unit = {

      val dataBatchLen = 1000
      val dftpTicket = DftpTicket.getDftpTicket(ticket)
      if(uriPool.exists(dftpTicket)){
        val dataFrame = uriPool.getDataFrame(dftpTicket)
        if(dataFrame.isEmpty) sendErrorWithFlightStatus(400, s"No DataFrame associated with ticket: $dftpTicket")
        else {
          try{
            sendDataFrame(dataFrame.get)
          }catch {
            case e: Exception =>
              logger.error(e)
              sendErrorWithFlightStatus(500, e.getMessage)
          }
        }
      }else {
        sendErrorWithFlightStatus(400, s"not found ticket $dftpTicket")
      }

      def sendDataFrame(dataFrame: DataFrame): Unit = {
        val schema = convertStructTypeToArrowSchema(dataFrame.schema)
        val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(schema, childAllocator)
        val loader = new VectorLoader(root)
        listener.start(root)
        dataFrame.mapIterator(iter => {
          val arrowFlightStreamWriter = ArrowFlightStreamWriter(iter)
          try {
            arrowFlightStreamWriter.process(root, dataBatchLen).foreach(batch => {
              try {
                loader.load(batch)
                while (!listener.isReady()) {
                  LockSupport.parkNanos(1)
                }
                listener.putNext()
              } finally {
                batch.close()
              }
            })
            while (!listener.isReady()) {
              LockSupport.parkNanos(1)
            }
            listener.completed()
          } catch {
            case e: Throwable => listener.error(e)
              e.printStackTrace()
              throw e
          } finally {
            iter.close()
            if (root != null) root.close()
            if (childAllocator != null) childAllocator.close()
          }
        })
      }
    }

    override def acceptPut(
                            callContext: FlightProducer.CallContext,
                            flightStream: FlightStream,
                            ackStream: FlightProducer.StreamListener[PutResult]
                          ): Runnable = {
      val dftpTicket: DftpTicket = flightStream.getDescriptor().getPath().get(0)
      new Runnable {
        override def run(): Unit = {
          val response = new DftpPutStreamResponse {
            override def sendError(code: Int, message: String): Unit = sendErrorWithFlightStatus(code, message)

            override def onNext(json: String): Unit = {
              try {
                val bytes = CodecUtils.encodeString(json)
                val buf: ArrowBuf = allocator.buffer(bytes.length)
                try {
                  buf.writeBytes(bytes)
                  ackStream.onNext(PutResult.metadata(buf))
                } finally
                  buf.close()
              } catch {
                case e: Throwable =>
                  e.printStackTrace()
                  ackStream.onError(e)
              }
            }

            override def onCompleted(): Unit = ackStream.onCompleted()
          }

          val request: DftpPutStreamRequest = dftpTicket match {
            case putTicket if putDataFrameParametersCache.contains(putTicket) =>
              new DftpPutDataFrameRequest {
                override def getDataFrame(): DataFrame = {
                  var schema = StructType.empty
                  if (flightStream.next()) {
                    val root = flightStream.getRoot
                    schema = ServerUtils.arrowSchemaToStructType(root.getSchema)
                    val stream = ServerUtils.flightStreamToRowIterator(flightStream)
                    DefaultDataFrame(schema, ClosableIterator(stream)())
                  } else {
                    DefaultDataFrame(schema, Iterator.empty)
                  }
                }

                override def getUserPrincipal(): UserPrincipal =
                  authenticatedUserMap.get(callContext.peerIdentity())

                override def getRequestParameters(): JSONObject =
                  putDataFrameParametersCache.get(putTicket).get
              }
            case putTicket if putBlobParametersCache.contains(putTicket) =>
              new DftpPutBlobRequest {
                override def getBlob(): Blob = new Blob {
                  private val stream: Iterator[Array[Byte]] = ServerUtils.flightStreamToRowIterator(flightStream)
                    .map(row => row.get(0) match {
                      case arr: Array[Byte] => arr
                      case other => throw new IllegalArgumentException(
                        s"Expected Array[Byte], but got ${other.getClass}"
                      )
                    })
                  override def offerStream[T](consume: InputStream => T): T = {
                    val inputStream = if(flightStream.next()) {
                      DataUtils.convertIteratorToInputStream(stream)
                    }else InputStream.nullInputStream()
                    consume(inputStream)
                  }

                  override def openByteStream(): ClosableIterator[Array[Byte]] = ClosableIterator(stream)()
                }

                override def getUserPrincipal(): UserPrincipal =
                  authenticatedUserMap.get(callContext.peerIdentity())

                override def getRequestParameters(): JSONObject =
                  putBlobParametersCache.get(putTicket).get
              }
          }

          kernelModule.putStream(request, response)
        }
      }
    }

    override def getFlightInfo(context: FlightProducer.CallContext,
                               descriptor: FlightDescriptor): FlightInfo = {
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
      val schema = new Schema(List.empty.asJava)
      new FlightInfo(schema, descriptor, List(flightEndpoint).asJava, -1L, 0L)
    }

    override def listFlights(context: FlightProducer.CallContext,
                             criteria: Criteria,
                             listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
      listener.onCompleted()
    }
  }

  private case class ArrowFlightStreamWriter(stream: Iterator[Row]) {

    def process(root: VectorSchemaRoot, batchSize: Int): Iterator[ArrowRecordBatch] = {
      stream.grouped(batchSize).map(rows => createDummyBatch(root, rows))
    }

    private def createDummyBatch(arrowRoot: VectorSchemaRoot, rows: Seq[Row]): ArrowRecordBatch = {
      arrowRoot.allocateNew()
      val fieldVectors = arrowRoot.getFieldVectors.asScala
      var i = 0
      rows.foreach(row => {
        var j = 0
        fieldVectors.foreach(vec => {
          val value = row.get(j)
          value match {
            case v: Int => vec.asInstanceOf[IntVector].setSafe(i, v)
            case v: Long => vec.asInstanceOf[BigIntVector].setSafe(i, v)
            case v: Double => vec.asInstanceOf[Float8Vector].setSafe(i, v)
            case v: Float => vec.asInstanceOf[Float4Vector].setSafe(i, v)
            case v: java.math.BigDecimal => vec.asInstanceOf[VarCharVector].setSafe(i, v.toString.getBytes("UTF-8"))
            case v: String =>
              val bytes = v.getBytes("UTF-8")
              vec.asInstanceOf[VarCharVector].setSafe(i, bytes)
            case v: Boolean => vec.asInstanceOf[BitVector].setSafe(i, if (v) 1 else 0)
            case v: Array[Byte] => vec.asInstanceOf[VarBinaryVector].setSafe(i, v)
            case null => vec.setNull(i)
            case v: URIRef =>
              val bytes = v.getUrl.getBytes("UTF-8")
              vec.asInstanceOf[VarCharVector].setSafe(i, bytes)
            case _ => throw new UnsupportedOperationException("Type not supported")
          }
          j += 1
        })
        i += 1
      })
      arrowRoot.setRowCount(rows.length)
      val unloader = new VectorUnloader(arrowRoot)
      unloader.getRecordBatch
    }
  }

  private class URIReferencePool {

    private val dataFrameCache = TrieMap[String, DataFrame]()
    private val ticketExpiryDateCache = TrieMap[String, Long]()

    def registryDataFrame(dataFrame: DataFrame, expiryDate: Long = -1L): DftpTicket = {
      val dataFrameId = UUID.randomUUID().toString
      dataFrameCache.put(dataFrameId, dataFrame)
      ticketExpiryDateCache.put(dataFrameId, expiryDate)
      dataFrameId
    }

    def registryBlob(blob: Blob, expiryDate: Long = -1L): DftpTicket = {
      val blobId = UUID.randomUUID().toString
      val dataFrame = DataUtils.blobToDataFrame(blob)
      dataFrameCache.put(blobId, dataFrame)
      ticketExpiryDateCache.put(blobId, expiryDate)
      blobId
    }

    def exists(ticket: DftpTicket): Boolean = {
      dataFrameCache.keys.toList.contains(ticket)
    }

    def getDataFrame(ticket: DftpTicket): Option[DataFrame] = {
      val expiryDate = ticketExpiryDateCache.get(ticket)
      if(expiryDate.isEmpty) throw new TicketNotFoundException(ticket)
      else if(expiryDate.get < System.currentTimeMillis() && expiryDate.get != -1L) {
        throw new TicketExpiryException(ticket, expiryDate.get)
      }else dataFrameCache.get(ticket)
    }

    def cleanUp(): Unit = {
      dataFrameCache.values.foreach(df => df.mapIterator[Unit](iter => iter.close()))
      dataFrameCache.clear()
      ticketExpiryDateCache.clear()
    }

  }
}

object DftpServer {
  def start(config: DftpServerConfig, modulesDefined: Array[DftpModule]): DftpServer = {
    val server = new DftpServer(config) {
      modulesDefined.foreach(modules.addModule(_))
    }

    server.start()
    server
  }

  def start(configXmlFile: File): DftpServer = {
    val server = createDftpServer(configXmlFile)
    server.start()
    server
  }

  def startBlocking(configXmlFile: File): Unit = {
    createDftpServer(configXmlFile).startBlocking()
  }

  private def createDftpServer(configXmlFile: File): DftpServer = {
    val configDir = configXmlFile.getParentFile.getAbsolutePath
    System.setProperty("configDir", configDir)
    System.setProperty("serverHome", configXmlFile.getParentFile.getParentFile.getAbsolutePath)

    val context = new GenericApplicationContext()
    val reader = new XmlBeanDefinitionReader(context)
    reader.loadBeanDefinitions(new FileUrlResource(configXmlFile.getAbsolutePath))
    context.refresh()

    val configBean = context.getBean(classOf[DftpServerConfigBean])
    val config: DftpServerConfig = configBean.toDftpServerConfig

    new DftpServer(config) {
      configBean.modules.foreach(modules.addModule(_))
    }
  }
}