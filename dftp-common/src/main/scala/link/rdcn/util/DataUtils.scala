package link.rdcn.util

import link.rdcn.Logging
import link.rdcn.struct.ValueType.{BinaryType, BooleanType, DoubleType, FloatType, IntType, LongType, NullType, RefType, StringType}
import link.rdcn.struct.{Blob, ClosableIterator, Column, DataFrame, DefaultDataFrame, Row, StructType, URIRef, ValueType}
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{Cell, CellType, DateUtil}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.json.JSONObject

import java.io.{File, FileInputStream, IOException, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.io.Source

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 10:00
 * @Modified By:
 */
object DataUtils extends Logging{

  def getStructTypeFromMap(row: Map[String, Any]): StructType = {
    StructType(row.map(row => Column(row._1, inferValueType(row._2))).toSeq)
  }

  def getDataFrameByStream(stream: Iterator[Row]): DefaultDataFrame = {
    if (stream.isEmpty) return DefaultDataFrame(StructType.empty, ClosableIterator(Iterator.empty)(() => ()))
    val row = stream.next()
    val structType = inferSchemaFromRow(row)
    stream match {
      case iter: ClosableIterator[Row] => DefaultDataFrame(structType,
        ClosableIterator(Seq(row).iterator ++ stream)(iter.close))
      case _ => DefaultDataFrame(structType, ClosableIterator(Seq(row).iterator ++ stream)(() => ()))
    }
  }

  def listAllFilesWithAttrs(directoryFile: File): Iterator[(File, BasicFileAttributes)] = {
    def walk(file: File): Iterator[File] = {
      if (file.isDirectory) {
        // 避免 null 的情况：listFiles 返回 null 时用空数组代替
        Option(file.listFiles()).getOrElse(Array.empty).iterator.flatMap(walk)
      } else if (file.isFile) {
        Iterator.single(file)
      } else {
        Iterator.empty
      }
    }

    walk(directoryFile).flatMap { file =>
      val path = file.toPath
      try {
        val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])
        Some((file, attrs))
      } catch {
        case _: IOException =>
          logger.error(s"读取文件 ${file.getAbsolutePath} 失败")
          None
      }
    }
  }

  def listFilesWithAttributes(directoryFile: File): Seq[(File, BasicFileAttributes)] = {
    if (directoryFile.exists() && directoryFile.isDirectory) {
      directoryFile.listFiles()
        .toSeq
        .flatMap { file =>
          val path = file.toPath
          try {
            val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])
            Some((file, attrs))
          } catch {
            case _: IOException =>
              logger.error(s"读取文件${file.getAbsolutePath} 失败")
              None
          }
        }
    } else {
      Seq.empty
    }
  }

  def getFileType(file: File): String = {
    var inputStream:FileInputStream = null
    try {
      inputStream = new FileInputStream(file)
      MimeTypeFactory.guessMimeTypeWithPrefix(inputStream).text
    } catch {
      case _: Exception => "Unknown type"
    } finally {
      inputStream.close()
    }
  }

  def getFileLines(filePath: String): Iterator[String] = {
    val source = Source.fromFile(filePath)
    source.getLines()
  }

  def getFileLines(file: File): ClosableIterator[String] = {
    val source = Source.fromFile(file)
    ClosableIterator(source.getLines())(source.close())
  }

  def convertStringRowToTypedRow(row: Row, schema: StructType): Row = {
    val typedValues = schema.columns.zipWithIndex.map { case (field, i) =>
      val rawValue = row.getAs[String](i) // 原始 String 值
      if (rawValue == null) {
        null
      } else field.colType match {
        case IntType => rawValue.toInt
        case LongType => rawValue.toLong
        case DoubleType => rawValue.toDouble
        case FloatType => rawValue.toFloat
        case BooleanType => rawValue.toBoolean
        case StringType => rawValue
        // 可以继续扩展其他类型
        case _ => throw new UnsupportedOperationException(s"Unsupported type: ${field.colType}")
      }
    }
    Row.fromSeq(typedValues)
  }

  /** 推断多列的类型（每列保留最大兼容类型） */
  def inferSchema(lines: Seq[Array[String]], header: Seq[String]): StructType = {
    if (lines.isEmpty)
      return StructType.empty

    val numCols = lines.head.length

    // 如果 header 为空，则自动生成 col_0, col_1, ...
    val columnNames: Seq[String] =
      if (header.isEmpty)
        Array.tabulate(numCols)(i => s"_${i + 1}")
      else
        header

    // transpose: 行列互换以便按列推断类型
    val transposed: Seq[Seq[String]] = lines.transpose.map(_.toSeq)

    val types: Seq[ValueType] = transposed.map { colValues =>
      val guessedTypes = colValues.map(inferStringValueType)
      if (guessedTypes.contains(StringType)) StringType
      else if (guessedTypes.contains(DoubleType)) DoubleType
      else if (guessedTypes.contains(LongType)) LongType
      else BooleanType
    }

    StructType.fromSeq(columnNames.zip(types).map { case (name, vt) => Column(name.trim, vt) })
  }

  /** 推断string的类型 */
  def inferStringValueType(value: String): ValueType = {
    if (value == null || value.isEmpty) StringType
    else if (value.matches("^-?\\d+$")) LongType
    else if (value.matches("^-?\\d+\\.\\d+$")) DoubleType
    else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) BooleanType
    else StringType
  }


  /** 底层流式统计一个file的行数 */
  def countLinesFast(file: File): Long = {
    val reader = Files.newBufferedReader(file.toPath, StandardCharsets.UTF_8)
    try {
      reader.lines().count()
    } finally {
      reader.close()
    }
  }

  def inferSchemaFromRow(row: Row): StructType = {
    val columns = row.values.zipWithIndex.map { case (value, idx) =>
      val name = s"_${idx + 1}"
      val valueType = inferValueType(value)
      Column(name, valueType)
    }
    StructType.fromSeq(columns)
  }

  def inferValueType(value: Any): ValueType = value match {
    case null => NullType
    case _: Int => IntType
    case _: Long => LongType
    case _: Double | _: Float => DoubleType
    case _: Boolean => BooleanType
    case _: Array[Byte] => BinaryType
    case _: java.io.File => BinaryType
    case _: URIRef => RefType
    case _ => StringType
  }

  /** 推断 schema，只读取前两行 */
  def inferExcelSchema(path: String): StructType = {
    val workbook = new XSSFWorkbook(new FileInputStream(path))
    val sheet = workbook.getSheetAt(0)
    val rowIter = sheet.iterator().asScala

    if (!rowIter.hasNext) throw new RuntimeException("Empty Excel file")
    val headerRow = rowIter.next()
    val headers = headerRow.cellIterator().asScala.map(_.toString.trim).toList

    if (!rowIter.hasNext) throw new RuntimeException("No data row to infer types")
    val typeSampleRow = rowIter.next()
    val inferredTypes = typeSampleRow.cellIterator().asScala.toList.map(detectType)

    val finalTypes = headers.zipAll(inferredTypes, "", ValueType.StringType).map(_._2)
    StructType.fromSeq(headers.zip(finalTypes).map { case (n, t) => Column(n, t) })
  }

  def getExcelRowNum(path: String): Int = {
    val fis = new FileInputStream(path)
    val workbook = new XSSFWorkbook(fis)
    val sheet = workbook.getSheetAt(0)
    val rowCount = sheet.getPhysicalNumberOfRows
    workbook.close()
    fis.close()
    rowCount - 1
  }

  /** 按 schema 读取所有数据为 Iterator[List[Any]] */
  def readExcelRows(path: String, schema: StructType): Iterator[List[Any]] = {
    val workbook = if (path.toLowerCase().endsWith(".xlsx")) {
       new XSSFWorkbook(new FileInputStream(path)); // .xlsx 文件
    } else if (path.toLowerCase().endsWith(".xls")) {
       new HSSFWorkbook(new FileInputStream(path)); // .xls 文件
    } else {
      throw new IllegalArgumentException("不支持的文件格式");
    }
    val sheet = workbook.getSheetAt(0)
    val rowIter = sheet.iterator().asScala

    if (!rowIter.hasNext) throw new RuntimeException("Empty Excel file")
    rowIter.next() // 跳过 header 行

    val headers = schema.columnNames
    val types = schema.columns.map(_.colType)

    rowIter.map { row =>
      headers.indices.map { idx =>
        val cell = row.getCell(idx, org.apache.poi.ss.usermodel.Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        if (cell == null) "" else readCell(cell, types(idx))
      }.toList
    }
  }

  def getStructTypeStreamFromJson(iter: Iterator[String]): (Iterator[Row], StructType) = {
    if (iter.hasNext) {
      val firstLine = iter.next()
      val jo = new JSONObject(firstLine)
      val structType: StructType = StructType(jo.keys().asScala.map(key => Column(key, inferValueType(jo.get(key)))).toSeq)
      val stream: Iterator[Row] = iter.map(Row.fromJsonString(_)) ++ Seq(Row.fromJsonString(firstLine)).iterator
      (stream, structType)
    } else (Iterator.empty, StructType.empty)
  }

  def chunkedIterator(in: InputStream, chunkSize: Int = 10 * 1024 * 1024): Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
    private var finished = false

    override def hasNext: Boolean = !finished

    override def next(): Array[Byte] = {
      if (finished) throw new NoSuchElementException("No more data")

      val buffer = new Array[Byte](chunkSize)
      var bytesRead = 0
      while (bytesRead < chunkSize) {
        val read = in.read(buffer, bytesRead, chunkSize - bytesRead)
        if (read == -1) {
          finished = true
          // 返回实际读到的长度
          return if (bytesRead == 0) Iterator.empty.next() else buffer.take(bytesRead)
        }
        bytesRead += read
      }
      buffer
    }
  }

  def convertIteratorToInputStream(iterator: Iterator[Array[Byte]]): IteratorInputStream = new IteratorInputStream(iterator)

  class IteratorInputStream(iterator: Iterator[Array[Byte]]) extends InputStream {
    private var currentChunk: Array[Byte] = Array.emptyByteArray
    private var index: Int = 0

    /** 从流中读取一个字节 */
    override def read(): Int = {
      if (currentChunk == null) return -1
      if (index >= currentChunk.length) {
        if (iterator.hasNext) {
          currentChunk = iterator.next()
          index = 0
          read()
        } else {
          currentChunk = null
          -1
        }
      } else {
        val b = currentChunk(index) & 0xff
        index += 1
        b
      }
    }

    /** 从流中读取多个字节到缓冲区 */
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (currentChunk == null) return -1
      var totalRead = 0
      while (totalRead < len) {
        if (index >= currentChunk.length) {
          if (iterator.hasNext) {
            currentChunk = iterator.next()
            index = 0
          } else {
            return if (totalRead == 0) -1 else totalRead
          }
        }
        val bytesToCopy = math.min(len - totalRead, currentChunk.length - index)
        System.arraycopy(currentChunk, index, b, off + totalRead, bytesToCopy)
        index += bytesToCopy
        totalRead += bytesToCopy
      }
      totalRead
    }
  }

  def blobToDataFrame(blob: Blob): DataFrame = {
    val schema = StructType.blobStreamStructType
    DefaultDataFrame(schema, blob.openByteStream().map(bytes => Row.fromSeq(Seq(bytes))))
  }

  def dataFrameToBlob(df: DataFrame): Blob = {
    new Blob {
      override def offerStream[T](consume: InputStream => T): T = {
        val inputStream = df.mapIterator[InputStream](iter => {
          val chunkIterator = iter.map(value => {
            assert(value.values.length == 1)
            value._1 match {
              case v: Array[Byte] => v
              case other => throw new Exception(s"Blob parsing failed: expected Array[Byte], but got ${other}")
            }
          })
          DataUtils.convertIteratorToInputStream(chunkIterator)
        })
        consume(inputStream)
      }

      override def openByteStream(): ClosableIterator[Array[Byte]] =
        df.mapIterator[ClosableIterator[Array[Byte]]](iter => {
          val stream = iter.map(row => row.get(0) match {
            case bytes: Array[Byte] => bytes
            case other => throw new IllegalArgumentException(s"except Array[Byte] but $other")
          })
          ClosableIterator(stream)(iter.close())
        })

    }
  }

  private def detectType(cell: Cell): ValueType = {
    cell.getCellType match {
      case CellType.NUMERIC =>
        if (DateUtil.isCellDateFormatted(cell)) ValueType.LongType
        else {
          val v = cell.getNumericCellValue
          if (v == v.toInt) ValueType.IntType
          else if (v == v.toLong) ValueType.LongType
          else ValueType.DoubleType
        }
      case CellType.BOOLEAN => ValueType.BooleanType
      case _ => ValueType.StringType
    }
  }

  private def readCell(cell: Cell, valueType: ValueType): Any = {
    valueType match {
      case ValueType.IntType => cell.getNumericCellValue.toInt
      case ValueType.LongType => cell.getNumericCellValue.toLong
      case ValueType.DoubleType => cell.getNumericCellValue
      case ValueType.BooleanType => cell.getBooleanCellValue
      case _ => cell.toString.trim
    }
  }
}
