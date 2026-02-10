package link.rdcn.dacp.optree

import link.rdcn.{JobFlowLogger, Logging}
import link.rdcn.dacp.optree.fifo.FileType.FileType
import link.rdcn.dacp.optree.fifo._
import link.rdcn.dacp.recipe.{Transformer11, Transformer21}
import link.rdcn.operation.{ExecutionContext, FlowGenericFunctionCall, FunctionSerializer, FunctionWrapper, GenericFunctionCall}
import link.rdcn.struct.ValueType.BinaryType
import link.rdcn.struct._
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.getDataFrameByStream
import org.json.{JSONArray, JSONObject}

import java.io._
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Paths}
import java.util
import java.util.{Base64, ServiceLoader, UUID}
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter, asScalaIteratorConverter, mapAsScalaMapConverter}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/18 11:16
 * @Modified By:
 */
trait TransformFunctionWrapper extends FunctionWrapper with Logging {
  def toJson: JSONObject

  def applyToInput(input: Any, ctx: ExecutionContext): Any = {
    require(input.isInstanceOf[Seq[DataFrame]] && ctx.isInstanceOf[FlowExecutionContext])
    applyToDataFrames(input.asInstanceOf[Seq[DataFrame]], ctx.asInstanceOf[FlowExecutionContext])
  }

  def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame
}

object TransformFunctionWrapper {
  def fromJsonObject(jo: JSONObject): TransformFunctionWrapper = {
    jo.getString("type") match {
      case LangTypeV2.PYTHON_CODE.name => PythonCode(jo.getString("code"))
      case LangTypeV2.JAVA_BIN.name => JavaBin(jo.getString("serializedBase64"))
      case LangTypeV2.JAVA_CODE.name => JavaCode(jo.getString("javaCodeString"))
      case LangTypeV2.PYTHON_BIN.name => PythonBin(jo.getString("functionName"), jo.getString("whlPath"), jo.getInt("batchSize"))
      case LangTypeV2.JAVA_JAR.name => JavaJar(jo.getString("jarPath"), jo.getString("functionName"),
        jo.getString("className"), jo.getJSONObject("params"), jo.getString("id"))
      case LangTypeV2.CPP_BIN.name => CppBin(jo.getString("cppPath"))
      case LangTypeV2.REPOSITORY_OPERATOR.name => RepositoryOperator(jo.getString("functionName"),
        Try(jo.getString("functionVersion")).toOption, jo.getJSONObject("params"), jo.getString("id"))
      case LangTypeV2.FILE_REPOSITORY_BUNDLE.name => {
        val command = jo.getJSONArray("command").toList.asScala.map(_.toString)
        val inputFilePath = jo.getJSONArray("inputFilePath").toList.asScala
          .map(_.asInstanceOf[util.HashMap[String, FileType]])
          .map(jo => (jo.get("filePath").toString, FileType.fromString(jo.get("fileType").toString)))
        var outputFileType = FileType.FIFO_BUFFER
        val outPutFilePath = jo.getJSONArray("outputFilePath").toList.asScala
          .map(_.asInstanceOf[util.HashMap[String, FileType]])
          .map { jo =>
            outputFileType = FileType.fromString(jo.get("fileType").toString)
            (jo.get("filePath").toString, outputFileType)
          }
        val dockerContainer = DockerContainer.fromJson(jo.getJSONObject("dockerContainer"))
        if (outputFileType == FileType.FIFO_BUFFER)
          FifoFileRepositoryBundle(command, inputFilePath, outPutFilePath, dockerContainer)
        else
          TempFileRepositoryBundle(command, inputFilePath, outPutFilePath, dockerContainer)
      }
    }
  }

  def getJavaSerialized(functionCall: FlowGenericFunctionCall): JavaBin = {
    val objectBytes = FunctionSerializer.serialize(functionCall)
    val base64Str: String = java.util.Base64.getEncoder.encodeToString(objectBytes)
    JavaBin(base64Str)
  }
}

case class PythonCode(code: String, batchSize: Int = 100) extends TransformFunctionWrapper {

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.PYTHON_CODE.name)
    jo.put("code", code)
  }

  override def toString(): String = "PythonCodeNode Function"

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val stream = inputs.head.mapIterator[Iterator[Row]](iter => {
      new Iterator[Row] {
        private val grouped: Iterator[Seq[Row]] = iter.grouped(batchSize)

        private var currentBatchIter: Iterator[Row] = Iterator.empty

        override def hasNext: Boolean = {
          while (!currentBatchIter.hasNext && grouped.hasNext) {
            val batch = grouped.next()

            // Convert Seq[Row] => java.util.List[java.util.List[AnyRef]]
            val javaBatch = new java.util.ArrayList[java.util.List[AnyRef]]()
            for (row <- batch) {
              val rowList = new java.util.ArrayList[AnyRef]()
              row.toSeq.foreach(v => rowList.add(v.asInstanceOf[AnyRef]))
              javaBatch.add(rowList)
            }
            val interp = ctx.getSharedInterpreter().getOrElse(throw new Exception("Failed to load SharedInterpreter"))
            interp.set("input_data", javaBatch)
            interp.exec(code)
            val result = interp.getValue("output_data", classOf[java.util.List[java.util.List[AnyRef]]])
            val scalaRows = result.asScala.map(Row.fromJavaList)
            currentBatchIter = scalaRows.iterator
          }

          currentBatchIter.hasNext
        }

        override def next(): Row = {
          if (!hasNext) throw new NoSuchElementException("No more rows")
          currentBatchIter.next()
        }
      }
    })
    getDataFrameByStream(stream)
  }
}

case class JavaBin(serializedBase64: String) extends TransformFunctionWrapper with Logging {

  lazy val flowGenericFunctionCall: FlowGenericFunctionCall = {
    val restoredBytes = java.util.Base64.getDecoder.decode(serializedBase64)
    FunctionSerializer.deserialize(restoredBytes).asInstanceOf[FlowGenericFunctionCall]
  }

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.JAVA_BIN.name)
    jo.put("serializedBase64", serializedBase64)
  }

  override def toString(): String = "Java_bin Function"

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val jobFlowLogger = flowLogger(ctx.getJobId(), "")
    val params = new JSONObject()
    inputs.length match {
      case 1 => flowGenericFunctionCall.transform(inputs.head, jobFlowLogger, params).asInstanceOf[DataFrame]
      case 2 => flowGenericFunctionCall.transform((inputs.head, inputs.last), jobFlowLogger, params).asInstanceOf[DataFrame]
      case other => throw new IllegalArgumentException(s"Unsupported inputs DataFrames length: $other")
    }
  }
}

case class JavaCode(javaCodeString: String) extends TransformFunctionWrapper {

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.JAVA_CODE.name)
    jo.put("javaCodeString", javaCodeString)
  }

  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    //TODO 支持对一组DataFrame的处理
    input.head.mapIterator[DataFrame](iter => {
      val clazzMap = FunctionSerializer.deserialize(Base64.getDecoder.decode(javaCodeString)).asInstanceOf[java.util.Map[String, Array[Byte]]]
      val classLoader = new ByteArrayClassLoader(clazzMap.asScala.toMap, Thread.currentThread().getContextClassLoader)
      val mainClassName = clazzMap.asScala.keys.find(!_.contains("$"))
        .getOrElse(throw new RuntimeException("cannot find main class name"))
      val clazz = classLoader.loadClass(mainClassName)
      val instance = clazz.getDeclaredConstructor().newInstance()
      val method = clazz.getMethod("transform", classOf[DataFrame])
      method.invoke(instance, getDataFrameByStream(iter)).asInstanceOf[DataFrame]
    })
  }

  private class ByteArrayClassLoader(classBytes: Map[String, Array[Byte]], parent: ClassLoader) extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      // 检查当前类加载器是否已经加载过这个类
      val loadedClass = findLoadedClass(name)
      if (loadedClass != null) {
        return loadedClass
      }

      // 尝试从传入的字节码 Map 中查找
      classBytes.get(name) match {
        case Some(bytes) =>
          defineClass(name, bytes, 0, bytes.length)
        case None =>
          super.findClass(name)
      }
    }
  }
}

case class PythonBin(functionName: String, whlPath: String, batchSize: Int = 100) extends TransformFunctionWrapper {

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.PYTHON_BIN.name)
    jo.put("functionName", functionName)
    jo.put("whlPath", whlPath)
    jo.put("batchSize", batchSize)
  }

  //TODO 支持对一组DataFrame的处理
  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val jep = ctx.getSubInterpreter(Paths.get(ctx.pythonHome,
        LangTypeV2.PYTHON_BIN.name + UUID.randomUUID()).toString, whlPath)
      .getOrElse(throw new IllegalArgumentException("Python interpreter is required"))
    jep.eval("import link.rdcn.operators.registry as registry")
    jep.set("operator_name", functionName)
    jep.eval("func = registry.get_operator(operator_name)")
    val stream = input.head.mapIterator(rows => {
      rows.grouped(batchSize).flatMap(rowSeq => {
        jep.set("input_rows", rowSeq.map(_.toSeq.asJava).asJava)
        jep.eval("output_rows = func(input_rows)")
        val result = jep.getValue("output_rows").asInstanceOf[java.util.List[java.util.List[Object]]]
        result.asScala.map(Row.fromJavaList(_))
      })
    })
    DataUtils.getDataFrameByStream(stream)
  }
}

case class JavaJar(jarPath: String, functionName: String, className: String, params: JSONObject, id: String)
  extends TransformFunctionWrapper with Logging
{
  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.JAVA_JAR.name)
      .put("jarPath", jarPath)
      .put("functionName", functionName)
      .put("className", className)
      .put("params", params)
      .put("id", id)
  }

  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val jarFile = new java.io.File(jarPath)
    val urls = Array(jarFile.toURI.toURL)
    val parentLoader = getClass.getClassLoader
    val pluginLoader = new PluginClassLoader(urls, parentLoader)
    val jobFlowLogger: JobFlowLogger = flowLogger(ctx.getJobId, id)
    functionName match {
      case "Transformer11" =>
        val serviceLoader = ServiceLoader.load(classOf[Transformer11], pluginLoader).iterator().asScala.toList
        serviceLoader.find(instance => instance.getClass.getName == className)
          .map(instance => instance.transform(input.head, jobFlowLogger, params)).getOrElse(
            throw new Exception(s"No $className Transformer11 implementation class was found in this jar $jarPath")
          )
      case "Transformer21" =>
        val serviceLoader = ServiceLoader.load(classOf[Transformer21], pluginLoader).iterator()
        if (!serviceLoader.hasNext) throw new Exception(s"No Transformer21 implementation class was found in this jar $jarPath")
        serviceLoader.next().transform((input.head, input.last), jobFlowLogger, params)
      case other => throw new IllegalArgumentException(s"Unsupported input function type: $other")
    }
  }

  private class PluginClassLoader(urls: Array[URL], parent: ClassLoader)
    extends URLClassLoader(urls, parent) {

    override def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
      // 必须由父加载器加载的类（避免 LinkageError）
      if (name.startsWith("scala.") ||
        name.startsWith("link.rdcn.") // 主程序中定义的接口、DataFrame等
      ) {
        return super.loadClass(name, resolve) // 委托给 parent
      }

      try {
        val clazz = findClass(name)
        if (resolve) resolveClass(clazz)
        clazz
      } catch {
        case _: ClassNotFoundException =>
          super.loadClass(name, resolve)
      }
    }
  }
}

case class CppBin(cppPath: String) extends TransformFunctionWrapper {

  override def toJson: JSONObject = new JSONObject().put("type", LangTypeV2.CPP_BIN.name)
    .put("cppPath", cppPath)

  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val execFile = new java.io.File(cppPath)

    if (execFile.exists() && !execFile.canExecute) {
      val succeed = execFile.setExecutable(true)
      if (!succeed) {
        throw new java.io.IOException(s"Failed to make file executable: $cppPath")
      }
    }
    val pb = new ProcessBuilder(execFile.getAbsolutePath)
    pb.redirectError(ProcessBuilder.Redirect.INHERIT)
    val process = pb.start()
    val writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    val inputDataFrame = input.head
    val inputSchema = inputDataFrame.schema
    inputDataFrame.mapIterator[DataFrame](iter => {
      val stream = new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          val row = iter.next()
          val jsonStr = row.toJsonString(inputSchema)
          try {
            writer.write(jsonStr)
            writer.newLine()
            writer.flush()

            val response = reader.readLine()
            if (response == null) {
              handleProcessDeath(process, "C++ process closed stream unexpectedly")
            }
            response
          } catch {
            case e: java.io.IOException =>
              handleProcessDeath(process, s"Write failed: ${e.getMessage}")
              throw e
          }
        }
      }
      val r = DataUtils.getStructTypeStreamFromJson(stream)
      val autoClosingIterator = ClosableIterator(r._1)(() => {
        iter.close()
        writer.close()
        reader.close()
        process.destroy()
      })
      DefaultDataFrame(r._2, autoClosingIterator)
    })
  }

  def handleProcessDeath(proc: Process, msg: String): Unit = {
    val exitCode = if (proc.isAlive) "Alive" else proc.exitValue().toString

    val stdoutResidue = try {
      val sb = new StringBuilder()
      while (proc.getInputStream.available() > 0) {
        val ch = proc.getInputStream.read()
        if (ch != -1) sb.append(ch.toChar)
      }
      sb.toString()
    } catch {
      case _: Exception => "Cannot read stdout"
    }

    val errorDetail =
      s"""
         |Error Context: $msg
         |Process Status: $exitCode
         |Last words from Stdout: [$stdoutResidue]
    """.stripMargin

    throw new RuntimeException(errorDetail)
  }
}

case class RepositoryOperator(functionName: String,
                              functionVersion: Option[String] = None,
                              params: JSONObject = new JSONObject(),
                              id: String = ""
                             ) extends TransformFunctionWrapper {

  override def toJson: JSONObject = new JSONObject().put("type", LangTypeV2.REPOSITORY_OPERATOR.name)
    .put("functionName", functionName)
    .put("functionVersion", functionVersion.orNull)
    .put("params", params)
    .put("id", id)

  var transformFunctionWrapper: TransformFunctionWrapper = _

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val transformFunctionWrapper = ctx.getRepositoryClient()
      .getOrElse(throw new Exception("Operator repository client not found. Please configure the client settings."))
      .parseTransformFunctionWrapper(functionName, functionVersion, params, ctx, id)
    this.transformFunctionWrapper = transformFunctionWrapper
    transformFunctionWrapper.applyToDataFrames(inputs, ctx)
  }
}

trait FileRepositoryBundle extends TransformFunctionWrapper {

  def command: Seq[String]

  def inputFilePath: Seq[(String, FileType)]

  def outputFilePath: Seq[(String, FileType)]

  def dockerContainer: DockerContainer

  override def toJson: JSONObject = {
    val jo = new JSONObject
    jo.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
    jo.put("command", new JSONArray(command.asJava))
    jo.put("inputFilePath", new JSONArray(inputFilePath.map(file =>
      new JSONObject().put("filePath", file._1)
        .put("fileType", file._2.toString)).asJava))
    jo.put("outputFilePath", new JSONArray(outputFilePath.map(file =>
      new JSONObject().put("filePath", file._1)
        .put("fileType", file._2.toString)).asJava))
    jo.put("dockerContainer", dockerContainer.toJson())
    jo
  }

  def runOperator(outputDataFrame: DataFrame): DataFrame = {
    DockerExecute.nonInteractiveExec(command.toArray, dockerContainer.containerName) //"jyg-container"
    dockerContainer.stop()
    if (outputFilePath.head._2 != FileType.DIRECTORY) {
      //TODO: support outputting multiple DataFrames
      outputDataFrame
    } else DataStreamSource.filePath(new File(outputFilePath.head._1)).dataFrame
  }

  def deleteFile(): Unit = {
    def safeDelete(pathStr: String): Unit = {
      if (pathStr != null && pathStr.nonEmpty) {
        try {
          val path = Paths.get(pathStr)
          if (Files.exists(path)) {
            val process = Runtime.getRuntime.exec(Array("rm", "-rf", pathStr))
            process.waitFor()
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to delete file: $pathStr", e)
        }
      }
    }

    (inputFilePath ++ outputFilePath).foreach(filePath => {
      safeDelete(filePath._1)
    })

    if (dockerContainer.hostPath.nonEmpty) {
      safeDelete(dockerContainer.hostPath.get)
    }
  }

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    dockerContainer.start()
    val outputFile = outputFilePath.map(path => {
      if (path._2 == FileType.DIRECTORY) {
        val dir = new File(path._1)
        dir.deleteOnExit()
        dir.mkdirs()
      } else FilePipe.fromFilePath(path._1, path._2)
    })
    require(inputs.length == inputFilePath.length,
      s"Operator requires ${inputFilePath.length} input file(s), but received ${inputs.length}.")
    inputs.zip(inputFilePath).foreach(dfAndInput => {
      dfAndInput._1 match {
        case f: FileDataFrame =>
          if (dfAndInput._2._1 != f.filePipe.path) {
            f.filePipe.copyToFile(FilePipe.fromFilePath(dfAndInput._2._1, dfAndInput._2._2))
          }
        case f: DataFrame =>
          if (f.schema.columns.length == 1 && f.schema.columns.head.colType == BinaryType) {
//            非结构化数据
            val file = new File(dfAndInput._2._1)
            val out = new BufferedOutputStream(new FileOutputStream(file))
            try {
              f.foreach { row =>
                row.get(0) match {
                  case bytes: Array[Byte] => out.write(bytes)
                  case other => throw new Exception(s"expect Array[Byte] but $other")
                }
              }
              out.flush()
            } finally {
              out.close()
            }
          } else if (f.schema == StructType.binaryStructType) {
//            文件夹
            val fileIndex = f.schema.indexOf("file").get
            val dir = Paths.get(dfAndInput._2._1).toFile
            dir.deleteOnExit()
            dir.mkdirs()
            f.foreach(row => {
              try{
                val fileUrl = row.getAs[URIRef](fileIndex)
                val blob = fileUrl.getBlob
                writeBlobToFile(blob, Paths.get(dfAndInput._2._1, row.getAs[String](0)).toFile)
              }catch {
                case e: Exception => logger.error(e)
              }
            })
          } else {
//            结构化数据默认csv数据
            if (dfAndInput._2._2 == FileType.FIFO_BUFFER) {
              val future = Future {
                FilePipe.fromFilePath(dfAndInput._2._1, dfAndInput._2._2)
                  .write(f.mapIterator(iter => Seq(f.schema.columns.map(_.name).mkString(",")).iterator ++ iter.map(row => row.toSeq.mkString(","))))
              }
              future onComplete {
                case Success(value) => logger.debug(s"load ${dfAndInput._2._1} success")
                case Failure(e) => logger.debug(s"load ${dfAndInput._2._1} faild")
                  throw e
              }
            } else {
//            MMAP文件
              FilePipe.fromFilePath(dfAndInput._2._1, dfAndInput._2._2)
                .write(f.mapIterator(iter => Seq(f.schema.columns.map(_.name)
                  .mkString(",")).iterator ++ iter.map(row => row.toSeq.mkString(","))))
            }
          }
      }
    })
    //TODO: support outputting multiple DataFrames
    if (outputFilePath.head._2 == FileType.DIRECTORY) {
      runOperator(DataStreamSource.filePath(new File(outputFilePath.head._1)).dataFrame)
    } else FileDataFrame(outputFile.head.asInstanceOf[FilePipe], outputFilePath.head._2)
  }

  private def writeBlobToFile(blob: Blob, file: File): Unit = {
    blob.offerStream { in =>
      val buffer = new Array[Byte](8 * 1024)
      val out = new BufferedOutputStream(new FileOutputStream(file))
      try {
        Iterator
          .continually(in.read(buffer))
          .takeWhile(_ != -1)
          .foreach(read => out.write(buffer, 0, read))
      } finally {
        in.close()
        out.close()
      }
    }
  }
}

case class FifoFileRepositoryBundle(command: Seq[String],
                                    inputFilePath: Seq[(String, FileType)],
                                    outputFilePath: Seq[(String, FileType)],
                                    dockerContainer: DockerContainer) extends FileRepositoryBundle

case class TempFileRepositoryBundle(command: Seq[String],
                                    inputFilePath: Seq[(String, FileType)],
                                    outputFilePath: Seq[(String, FileType)],
                                    dockerContainer: DockerContainer) extends FileRepositoryBundle