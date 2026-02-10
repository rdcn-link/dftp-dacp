package link.rdcn.dacp.catalog

import org.json.JSONObject

import scala.beans.BeanProperty

/**
 * @Author renhao
 * @Description:
 * @Data 2026/1/28 13:57
 * @Modified By:
 */
class DatasetAuthor extends Serializable {

  @BeanProperty
  var institution: String = _

  @BeanProperty
  var team: String = _

  @BeanProperty
  var authors: String = _

  def this(institution: String, team: String, authors: String) = {
    this()
    this.institution = institution
    this.team = team
    this.authors = authors
  }

  def toJSON(): JSONObject = {
    new JSONObject()
      .put("institution", getInstitution)
      .put("team", getTeam)
      .put("authors", getAuthors)
  }
}

object DatasetAuthor {

  def fromJSON(jo: JSONObject): DatasetAuthor = {
    val datasetAuthor = new DatasetAuthor()
    datasetAuthor.setAuthors(jo.optString("authors", null))
    datasetAuthor.setInstitution(jo.optString("institution", null))
    datasetAuthor.setTeam(jo.optString("team", null))
    datasetAuthor
  }

}

