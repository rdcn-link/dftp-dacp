package link.rdcn.dacp.catalog

import link.rdcn.dacp.catalog.DatasetMetaData.fmt
import org.json.{JSONArray, JSONObject}

import scala.beans.BeanProperty
import java.io.Serializable
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, List}
import java.util.{ArrayList, Date, List => JList}
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2026/1/28 13:57
 * @Modified By:
 */
class DatasetMetaData extends Serializable {

  @BeanProperty var titleZh: String = _
  @BeanProperty var titleEn: String = _
  @BeanProperty var cstr: String = _
  @BeanProperty var doi: String = _

  @BeanProperty var introductionZh: String = _
  @BeanProperty var introductionEn: String = _

  @BeanProperty var keywordsZh: String = _
  @BeanProperty var keywordsEn: String = _

  @BeanProperty var subject: String = _
  @BeanProperty var topic: String = _
  @BeanProperty var format: String = _

  @BeanProperty var coverUrl: String = _
  @BeanProperty var source: String = _

  @BeanProperty var authors: List[DatasetAuthor] = _

  @BeanProperty var shareMode: String = _
  @BeanProperty var copyRight: String = _

  @BeanProperty var dacpUrl: String = _
  @BeanProperty var byteSize: Long = _
  @BeanProperty var fileNumber: Int = _

  @BeanProperty var refreshRate: String = _
  @BeanProperty var path: String = _
  @BeanProperty var version: String = _

  @BeanProperty var publishAt: Date = _

  @BeanProperty var viewCount: Long = _
  @BeanProperty var downloadCount: Long = _

  def toJson(): JSONObject = {
    val obj = new JSONObject()

    obj.put("titleZh", getTitleZh)
    obj.put("titleEn", getTitleEn)
    obj.put("cstr", getCstr)
    obj.put("doi", getDoi)

    obj.put("introductionZh", getIntroductionZh)
    obj.put("introductionEn", getIntroductionEn)

    obj.put("keywordsZh", getKeywordsZh)
    obj.put("keywordsEn", getKeywordsEn)

    obj.put("subject", getSubject)
    obj.put("topic", getTopic)
    obj.put("fort", getFormat)

    obj.put("coverUrl", getCoverUrl)
    obj.put("source", getSource)

    // authors
    if (getAuthors != null) {
      val arr = new JSONArray()
      getAuthors.asScala.foreach(au => arr.put(au.toJSON()))
      obj.put("authors", arr)
    }

    obj.put("shareMode", getShareMode)
    obj.put("copyRight", getCopyRight)

    obj.put("dacpUrl", getDacpUrl)
    obj.put("byteSize", getByteSize)
    obj.put("fileNuer", getFileNumber)

    obj.put("refreshRate", getRefreshRate)
    obj.put("path", getPath)
    obj.put("version", getVersion)

    if (getPublishAt != null) {
      obj.put("publishAt", fmt.format(getPublishAt))
    }

    obj.put("viewCount", getViewCount)
    obj.put("downloadCount", getDownloadCount)

    obj
  }
}

object DatasetMetaData {

  val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def fromJSON(obj: JSONObject): DatasetMetaData = {
    val m = new DatasetMetaData

    m.setTitleZh(obj.optString("titleZh", null))
    m.setTitleEn(obj.optString("titleEn", null))
    m.setCstr(obj.optString("cstr", null))
    m.setDoi(obj.optString("doi", null))

    m.setIntroductionZh(obj.optString("introductionZh", null))
    m.setIntroductionEn(obj.optString("introductionEn", null))

    m.setKeywordsZh(obj.optString("keywordsZh", null))
    m.setKeywordsEn(obj.optString("keywordsEn", null))

    m.setSubject(obj.optString("subject", null))
    m.setTopic(obj.optString("topic", null))
    m.setFormat(obj.optString("format", null))

    m.setCoverUrl(obj.optString("coverUrl", null))
    m.setSource(obj.optString("source", null))

    // authors
    if (obj.has("authors")) {
      val arr = obj.getJSONArray("authors")
      val list: JList[DatasetAuthor] = new util.ArrayList()
      for (i <- 0 until arr.length()) {
        list.add(DatasetAuthor.fromJSON(arr.getJSONObject(i)))
      }
      m.setAuthors(list)
    }

    m.setShareMode(obj.optString("shareMode", null))
    m.setCopyRight(obj.optString("copyRight", null))

    m.setDacpUrl(obj.optString("dacpUrl", null))
    m.setByteSize(obj.optLong("byteSize", 0L))
    m.setFileNumber(obj.optInt("fileNumber", 0))

    m.setRefreshRate(obj.optString("refreshRate", null))
    m.setPath(obj.optString("path", null))
    m.setVersion(obj.optString("version", null))

    if (obj.has("publishAt")) {
      m.setPublishAt(fmt.parse(obj.getString("publishAt")))
    }

    m.setViewCount(obj.optLong("viewCount", 0L))
    m.setDownloadCount(obj.optLong("downloadCount", 0L))

    m
  }
}

