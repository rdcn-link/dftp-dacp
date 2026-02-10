package link.rdcn.struct

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/21 17:20
 * @Modified By:
 */
sealed trait StreamState {
  def isFailed: Boolean = this match {
    case StreamState.Failed(_) => true
    case _ => false
  }
}

object StreamState {
  case object Running extends StreamState

  case object Completed extends StreamState

  case class Failed(cause: Throwable) extends StreamState

  case object Closed extends StreamState
}


case class ClosableIterator[T](
                                underlying: Iterator[T],
                                val onClose: () => Unit,
                                val isFileList: Boolean = false
                              ) extends Iterator[T] with AutoCloseable {

  private var closed = false

  private val startTime: Long = System.currentTimeMillis()
  private var endTime: Long = -1L
  private var itemCount: Long = 0L

  @volatile
  private var state: StreamState = StreamState.Running

  def currentState: StreamState = state

  def isCompleted: Boolean = state == StreamState.Completed
  def isFailed: Boolean = state.isInstanceOf[StreamState.Failed]
  def hasMoreData: Boolean = state == StreamState.Running

  override def hasNext: Boolean = {
    if (closed || state != StreamState.Running) return false
    try {
      val more = underlying.hasNext
      if (!more && !isFileList) {
        state = StreamState.Completed
        close()
      }
      more
    } catch {
      case ex: Throwable =>
        state = StreamState.Failed(ex)
        close()
        throw ex
    }
  }


  override def next(): T = {
    if (!hasNext && !isFileList)
      throw new NoSuchElementException("next on empty iterator")

    try {
      val value = underlying.next()
      itemCount += 1

      if (!underlying.hasNext && !isFileList) {
        state = StreamState.Completed
        close()
      }
      value
    } catch {
      case ex: Throwable =>
        state = StreamState.Failed(ex)
        close()
        throw ex
    }
  }


  def itemsPerSecond: Double = {
    val elapsedSeconds = if(endTime == -1L) {
      (System.currentTimeMillis() - startTime) / 1000.0
    }else (System.currentTimeMillis() - endTime) / 1000.0
    itemCount.toDouble / elapsedSeconds
  }

  def consumeItems: Long = itemCount

  override def close(): Unit = {
    if (!closed) {
      endTime = System.currentTimeMillis()
      closed = true
      if (state == StreamState.Running) {
        state = StreamState.Closed
      }
      try onClose()
      catch {
        case ex: Throwable =>
          state = StreamState.Failed(ex)
          throw new Exception(s"[ClosableIterator] Error during close: ${ex.getMessage}", ex)
      }
    }
  }
}

object ClosableIterator {
  def apply[T](underlying: Iterator[T])(onClose: => Unit): ClosableIterator[T] =
    new ClosableIterator[T](underlying, () => onClose)
}
