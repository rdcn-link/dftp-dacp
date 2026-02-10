package link.rdcn.client

import link.rdcn.Logging
import link.rdcn.client.ClientUtils.convertStructTypeToArrowSchema
import link.rdcn.message.DftpTicket.DftpTicket
import link.rdcn.message.{ActionMethodType, DftpTicket}
import link.rdcn.operation._
import link.rdcn.struct.ValueType.RefType
import link.rdcn.struct._
import link.rdcn.user.Credentials
import link.rdcn.util.{CodecUtils, DataUtils}
import org.apache.arrow.flight.auth.ClientAuthHandler
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.json.JSONObject

import java.io.{File, InputStream}
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 19:47
 * @Modified By:
 */
class DftpClient(host: String, port: Int, useTLS: Boolean = false) extends Logging {

  def login(credentials: Credentials): Unit = {
    flightClient.authenticate(new FlightClientAuthHandler(credentials))
  }

  /**
   * Sends an action request to the faird server and returns the result.
   *
   * @param actionName the name of the action to be executed on the server
   * @param parameters JSON-formatted string representing the parameters of the action
   * @return a JSON-formatted string containing the execution result returned by the server
   */
  def doAction(actionName: String, parameters: String = new JSONObject().toString): ActionResult = {
    try{
      val actionResultIter = flightClient.doAction(new Action(actionName, CodecUtils.encodeString(parameters)))
      if(!actionResultIter.hasNext) ActionResult(500, """{"error":"no response from server"}""")
      else {
        val code = CodecUtils.decodeString(actionResultIter.next().getBody).toInt
        val result = if (actionResultIter.hasNext) CodecUtils.decodeString(actionResultIter.next().getBody) else new JSONObject().toString
        ActionResult(code, result)
      }
    } catch {
      case e: io.grpc.StatusRuntimeException =>
        logger.error(e)
        val code = mapFlightExceptionToStatusCode(e)
        ActionResult(code, s"""{"error":"${e.getMessage}"}""")
      case e: Exception =>
        logger.error(e)
        ActionResult(520, s"""{"error":"${e.getMessage}"}""")// Unknown
    }
  }

  def doAction(actionName: String, parameters: JSONObject): ActionResult = doAction(actionName, parameters.toString)

  private def mapFlightExceptionToStatusCode(e: Throwable): Int = e match {
    case e: io.grpc.StatusRuntimeException =>
      e.getStatus.getCode match {
        case io.grpc.Status.Code.INVALID_ARGUMENT => 400
        case io.grpc.Status.Code.UNAUTHENTICATED => 401
        case io.grpc.Status.Code.PERMISSION_DENIED => 403
        case io.grpc.Status.Code.NOT_FOUND => 404
        case io.grpc.Status.Code.DEADLINE_EXCEEDED => 408
        case io.grpc.Status.Code.ALREADY_EXISTS => 409
        case io.grpc.Status.Code.INTERNAL => 500
        case io.grpc.Status.Code.UNIMPLEMENTED => 501
        case io.grpc.Status.Code.UNAVAILABLE => 503
        case _ => 520 // Unknown
      }
    case _ => 520 // Unknown
  }

  protected def openDataFrame(transformOp: TransformOp): DataFrameHandle = {
    val responseJson = new JSONObject(doAction(ActionMethodType.GET, transformOp.toJsonString).result)

    new DataFrameHandle {
      override def getDataFrameMeta: DataFrameMetaData =
        DataFrameMetaData.fromJson(responseJson.getJSONObject("dataframeMetaData"))

      override def getDataFrameTicket: DftpTicket = responseJson.getString("ticket")
    }
  }

  def openDataFrame(url: String): DataFrameHandle = openDataFrame(SourceOp(url))

  def getTabular(url: String): DataFrame = get(url)

  def getTabular(dataFrameHandle: DataFrameHandle): DataFrame = {
    val getStreamFunc = () => getStream(dataFrameHandle.getDataFrameTicket)

    // Create LazyDefaultDataFrame with the lazy stream
    LazyDefaultDataFrame(
      dataFrameHandle.getDataFrameMeta.getDataFrameSchema,
      getStreamFunc
    )
  }

  def openBlob(url: String): BlobHandler = {
    val responseJson = doAction(ActionMethodType.GET, SourceOp(url).toJsonString).getResultJson()

    new BlobHandler {
      override def getBlobSize: Long = responseJson.getLong("size")

      override def getBlobType: String = responseJson.getString("ticket")

      override def getDataFrameTicket: DftpTicket = responseJson.getString("blobType")
    }

  }

  def getBlob(url: String): Blob = {
    val blobHandler = openBlob(url)

    new Blob {
      private val chunkIterator = getStream(blobHandler.getDataFrameTicket).map(value => {
        assert(value.values.length == 1)
        value._1 match {
          case v: Array[Byte] => v
          case other => throw new Exception(s"Blob parsing failed: expected Array[Byte], but got ${other}")
        }
      })

      override def offerStream[T](consume: InputStream => T): T = {
        val inputStream = DataUtils.convertIteratorToInputStream(chunkIterator)
        consume(inputStream)
      }

      override def openByteStream(): ClosableIterator[Array[Byte]] = ClosableIterator(chunkIterator)()
    }
  }

  def get(url: String): DataFrame =
    RemoteDataFrameProxy(SourceOp(validateUrl(url)), getStream, openDataFrame)

  def isAlive(): Boolean = {
    try {
      flightClient.listFlights(Criteria.ALL).iterator()
      true
    } catch {
      case _: FlightRuntimeException => false
    }
  }

  protected def validateUrl(url: String): String = {
    if (UrlValidator.isPath(url)) url
    else {
      UrlValidator.validate(url) match {
        case Right((prefixSchema, host, port, path)) => {
          if (host == this.host && port.getOrElse(3101) == this.port) url
          else
            throw new IllegalArgumentException(s"Invalid request URL: $url Expected format: $prefixSchema://${this.host}[:${this.port}]")
        }
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }
  }

  def put(blob: Blob, parameters: String): Iterator[String] = {
    blob.offerStream[Iterator[String]](inputStream => {
      val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream)
        .map(bytes => Row.fromSeq(Seq(bytes)))
      val schema = StructType.blobStreamStructType
      val df = DefaultDataFrame(schema, stream)
      putStream(df, putBlobParameters(parameters))
    })
  }

  def putBlobParameters(parameters: String): DftpTicket = {
    val responseJson = doAction(ActionMethodType.PUT_BLOB, parameters).result
    val jo = new JSONObject(responseJson)
    jo.getString("ticket")
  }

  def put(dataFrame: DataFrame, parameters: String): Iterator[String] =
    putStream(dataFrame, putDataFrameParameters(parameters))

  def putDataFrameParameters(parameters: String): DftpTicket = {
    val responseJson = doAction(ActionMethodType.PUT_DATAFRAME, parameters).result
    val jo = new JSONObject(responseJson)
    jo.getString("ticket")
  }

  private def putStream(dataFrame: DataFrame, dftpTicket: DftpTicket, dataBatchLen: Int = 100): Iterator[String] = {
    val arrowSchema = convertStructTypeToArrowSchema(dataFrame.schema)
    val childAllocator = allocator.newChildAllocator("putStream-data-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, childAllocator)
    val putListener = new SyncPutListener
    val writer = flightClient.startPut(FlightDescriptor.path(dftpTicket), root, putListener)
    val loader = new VectorLoader(root)
    dataFrame.mapIterator(iter => {
      val arrowFlightStreamWriter = ArrowFlightStreamWriter(iter)
      try {
        arrowFlightStreamWriter.process(root, dataBatchLen).foreach(batch => {
          try {
            loader.load(batch)
            while (!writer.isReady()) {
              LockSupport.parkNanos(1)
            }
            writer.putNext()
          } finally {
            batch.close()
          }
        })
        writer.completed()
        ClientUtils.parsePutListener(putListener)
      } catch {
        case e: Throwable => writer.error(e)
          throw e
      } finally {
        if (root != null) root.close()
        if (childAllocator != null) childAllocator.close()
      }
    })
  }

  def close(): Unit = {
    flightClient.close()
  }

  protected def getStream(ticket: DftpTicket): Iterator[Row] = {
    val flightStream = flightClient.getStream(DftpTicket.getTicket(ticket))
    val root: VectorSchemaRoot = flightStream.getRoot
    val fieldVectors = root.getFieldVectors.asScala

    new Iterator[Row] {

      private var rowIndex = 0
      private var hasBatch = flightStream.next()

      override def hasNext: Boolean = {
        if (!hasBatch) {
          flightStream.close()
          false
        } else if (rowIndex < root.getRowCount) {
          true
        } else {
          rowIndex = 0
          hasBatch = flightStream.next()
          hasNext
        }
      }

      override def next(): Row = {
        val values = fieldVectors.map { vec =>
          if (vec.isNull(rowIndex)) null
          else vec match {
            case v: IntVector => v.get(rowIndex)
            case v: BigIntVector => v.get(rowIndex)
            case v: Float8Vector => v.get(rowIndex)
            case v: BitVector => v.get(rowIndex) == 1
            case v: VarBinaryVector => v.get(rowIndex)
            case v: VarCharVector =>
              val str = new String(v.get(rowIndex))
              val meta = v.getField.getMetadata
              if (meta == null || meta.isEmpty) str
              else meta.get("logicalType") match {
                case RefType.name  => new URIRef {
                  override def getUrl: String = str

                  override def getBlob: Blob = DftpClient.this.getBlob(getUrl)

                  override def getDataFrame: DataFrame = DftpClient.this.getTabular(getUrl)
                }
                case other =>
                  throw new UnsupportedOperationException(s"Unsupported logicalType: $other")
              }
            case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
          }
        }
        rowIndex += 1
        Row.fromSeq(values)
      }
    }
  }

  private val location = {
    if (useTLS) {
      Location.forGrpcTls(host, port)
    } else
      Location.forGrpcInsecure(host, port)
  }

  private val allocator: BufferAllocator = new RootAllocator()
  protected val flightClient: FlightClient = FlightClient.builder(allocator, location).build()

  private class FlightClientAuthHandler(credentials: Credentials) extends ClientAuthHandler {

    private var callToken: Array[Byte] = _

    override def authenticate(clientAuthSender: ClientAuthHandler.ClientAuthSender,
                              iterator: java.util.Iterator[Array[Byte]]): Unit = {
      clientAuthSender.send(CodecUtils.encodeCredentials(credentials))
      try {
        callToken = iterator.next()
      } catch {
        case _: Exception => callToken = null
      }
    }

    override def getCallToken: Array[Byte] = callToken
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

  case class ActionResult(statusCode: Int, result: String) {
    def getResultJson(): JSONObject = new JSONObject(result)
  }
}


object DftpClient {
  def connect(url: String, credentials: Credentials = null): DftpClient = {
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3)
        if(credentials != null) client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }

  def connectTLS(url: String, tlsFile: File, credentials: Credentials = null): DftpClient = {
    System.setProperty("javax.net.ssl.trustStore", tlsFile.getAbsolutePath)
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3, true)
        if(credentials != null) client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }
}
