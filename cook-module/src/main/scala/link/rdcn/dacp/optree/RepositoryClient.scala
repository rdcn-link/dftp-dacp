package link.rdcn.dacp.optree

import cn.cnic.operatordownload.client.OperatorClient
import link.rdcn.dacp.optree.fifo.{DockerContainer, FileType}
import link.rdcn.dacp.utils.FileUtils
import org.json.{JSONArray, JSONObject}

import java.io.{File, FileOutputStream, IOException, InputStream}
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.language.postfixOps

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/30 17:03
 * @Modified By:
 */
trait OperatorRepository {
  def parseTransformFunctionWrapper(functionName: String,
                                    functionVersion: Option[String],
                                    params: JSONObject,
                                    ctx: FlowExecutionContext,
                                    id: String
                                   ): TransformFunctionWrapper
}

class RepositoryClient(host: String = "http://10.0.89.39", port: Int = 8090) extends OperatorRepository {

  override def parseTransformFunctionWrapper(functionName: String,
                                             functionVersion: Option[String],
                                             params: JSONObject,
                                             ctx: FlowExecutionContext,
                                             id: String
                                            ): TransformFunctionWrapper = {
    val client: OperatorClient = OperatorClient.connect(s"$host:$port", null)

    val operatorInfo = new JSONObject(client.getOperatorByNameAndVersion(functionName, functionVersion.orNull))
    if (operatorInfo.has("data") && operatorInfo.getJSONObject("data").getString("type") == "python-script") {
      val operatorImage = operatorInfo.getJSONObject("data").getString("nexusUrl")

      val inputCounter = new AtomicLong(0)
      val outputCounter = new AtomicLong(0)

      val inputJa = operatorInfo.getJSONObject("data").getJSONObject("specialProperties").getJSONArray("inputFiles")
      val inFiles =  (0 until inputJa.length).map(index => inputJa.getJSONObject(index))
        //subfix,fileType,inParam,paramType
        .map(jo => (jo.getString("name"), jo.getString("fileType"), jo.getString("paramDescription")))
        .map(file =>(file._1, file._2, file._3, s"input${inputCounter.incrementAndGet()}${file._1}"))
      val outputJa = operatorInfo.getJSONObject("data").getJSONObject("specialProperties").getJSONArray("outputFiles")
      val outFiles = (0 until outputJa.length).map(index => outputJa.getJSONObject(index))
        .map(jo => (jo.getString("name"), jo.getString("fileType"), jo.getString("paramDescription")))
        .map(file =>(file._1, file._2, file._3, s"output${outputCounter.incrementAndGet()}${file._1}"))

      val commands = operatorInfo.getJSONObject("data").getString("command").split(" ")

      val operationId = s"${functionName}_${UUID.randomUUID().toString}"
      val hostPath = FileUtils.getTempDirectory("", operationId)
      val containerPath = s"/$operationId"

      val commandsWithParams = commands ++ (inFiles ++ outFiles).flatMap(file => Seq(file._3, Paths.get(containerPath, file._4).toString))
      var outputFileType = FileType.FIFO_BUFFER

      val inputFiles = inFiles.map(file => (Paths.get(hostPath, file._4).toString, FileType.fromString(file._2)))
      val outputFiles = outFiles
        .map { file =>
          outputFileType = FileType.fromString(file._2)
          (Paths.get(hostPath, file._4).toString, outputFileType)
        }
      val dockerContainer = DockerContainer(functionName, Some(hostPath), Some(containerPath), Some(operatorImage))
      if (outputFileType == FileType.FIFO_BUFFER)
        FifoFileRepositoryBundle(commandsWithParams, inputFiles, outputFiles, dockerContainer)
      else
        TempFileRepositoryBundle(commandsWithParams, inputFiles, outputFiles, dockerContainer)
    } else {
      val operatorDir = ctx.fairdHome
      val inputStream: InputStream = client.downloadOperatorAsStream(functionName, functionVersion.get)
      val specialProperties = operatorInfo.getJSONObject("data").getJSONObject("specialProperties")
      operatorInfo.getJSONObject("data").getString("type") match {
        case LangTypeV2.JAVA_JAR.name =>
          val operatorFunctionName = specialProperties.getString("functionName")
          val className = specialProperties.getString("className")
          val packageFile = Paths.get(operatorDir, "lib", s"$functionName-${functionVersion.get}.jar").toFile
          saveInputStreamToFile(inputStream, packageFile)
          JavaJar(packageFile.getAbsolutePath, operatorFunctionName, className, params, id)
        case LangTypeV2.CPP_BIN.name =>
          val packageFile = Paths.get(operatorDir, "lib", s"$functionName-${functionVersion.get}.cpp").toFile
          saveInputStreamToFile(inputStream, packageFile)
          CppBin(packageFile.getAbsolutePath)
        case LangTypeV2.PYTHON_BIN.name =>
          val operatorFunctionName = specialProperties.getString("functionName")
          val packageFile = Paths.get(operatorDir, "lib", s"$functionName-${functionVersion.get}.whl").toFile
          PythonBin(operatorFunctionName, packageFile.getAbsolutePath)
        case _ => throw new IllegalArgumentException(s"Unsupported operator type: ${operatorInfo.get("type")}")

      }
    }
  }

  private def saveInputStreamToFile(inputStream: InputStream, file: File): Unit = {
    val outputStream = new FileOutputStream(file)
    val buffer = new Array[Byte](1024 * 8)
    var bytesRead = 0

    try {
      while ({ bytesRead = inputStream.read(buffer); bytesRead != -1 }) {
        outputStream.write(buffer, 0, bytesRead)
      }
      println(s"File saved at: ${file.getAbsoluteFile}")
    } catch {
      case e: IOException => println(s"Error while saving file: ${e.getMessage}")
    } finally {
      inputStream.close()
      outputStream.close()
    }
  }

}
