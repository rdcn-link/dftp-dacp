package link.rdcn.struct

import link.rdcn.util.{DataUtils, MimeTypeFactory}

import java.io.{File, FileInputStream, InputStream}

trait Blob {

  def offerStream[T](consume: InputStream => T): T

  def openByteStream(): ClosableIterator[Array[Byte]]

  def size: Long = offerStream[Long](inputStream => {
    var size = 0L
    val buffer = new Array[Byte](1024 * 8)
    var bytesRead = inputStream.read(buffer)
    while (bytesRead != -1) {
      size += bytesRead
      bytesRead = inputStream.read(buffer)
    }
    size
  })

  def blobType: String = offerStream[String](inputStream => {
    MimeTypeFactory.guessMimeType(inputStream).text
  })

  override def toString: String = s"Blob Value"
}

object Blob {
  def fromFile(file: File): Blob = {
    new Blob {
      override def size: Long = file.length()

      override def offerStream[T](consume: java.io.InputStream => T): T = {
        var stream: InputStream = null
        try {
          stream = new FileInputStream(file)
          consume(stream)
        } finally {
          if (stream != null) stream.close()
        }
      }

      override def openByteStream(): ClosableIterator[Array[Byte]] = {
        val inputStream = new FileInputStream(file)
        val iter = DataUtils.chunkedIterator(inputStream)
        ClosableIterator(iter)(inputStream.close())
      }
    }
  }
}