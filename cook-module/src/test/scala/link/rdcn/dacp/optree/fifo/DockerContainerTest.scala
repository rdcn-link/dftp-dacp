/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:10
 * @Modified By:
 */
package link.rdcn.dacp.optree.fifo

import link.rdcn.dacp.optree.fifo.DockerContainer
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull}
import org.junit.jupiter.api.Test

class DockerContainerTest {

  @Test
  def testJsonSerializationRoundTrip_Full(): Unit = {
    val original = DockerContainer("my-test-container", Some("/tmp/host"), Some("/app/data"), Some("python:3.9-slim"))
    val json = original.toJson()
    assertNotNull(json, "toJson() should return JSONObject")

    assertEquals("my-test-container", json.getString("containerName"), "containerName mismatch")
    val deserialized = DockerContainer.fromJson(json)
    assertEquals(original, deserialized, "Roundtrip failed")
  }

  @Test
  def testJsonSerializationRoundTrip_Minimal(): Unit = {
    val original = DockerContainer("minimal-container")
    val json = original.toJson()

    assertFalse(json.has("hostPath"), "Should not have hostPath")
    val deserialized = DockerContainer.fromJson(json)
    assertEquals(original, deserialized, "Roundtrip failed")
  }
}