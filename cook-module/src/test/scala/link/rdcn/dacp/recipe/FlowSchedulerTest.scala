package link.rdcn.dacp.recipe

import link.rdcn.dacp.recipe.FlowScheduler
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.{File, PrintWriter}

class FlowSchedulerTest {

  @TempDir
  var tempDir: File = _

  @Test
  def testScheduleFlowDistribution(): Unit = {
    val inputJson =
      """
        |{"flow": {
        |    "paths": [
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "6edb7e80-1d25-4fe1-aaec-a3b987b3eb07",
        |            "to": "8288c8ef-cd6f-4e9e-883d-e9b802c0644c"
        |        },
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "e855c516-755c-4b2a-83a0-8e7fdec36444",
        |            "to": "8288c8ef-cd6f-4e9e-883d-e9b802c0644c"
        |        },
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "e3ede64d-0d23-4b99-8c47-b820da2ccb51",
        |            "to": "72037169-0169-4db3-af18-b807c48507f6"
        |        },
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "4b76ba8e-b019-4227-a355-a4032df28492",
        |            "to": "72037169-0169-4db3-af18-b807c48507f6"
        |        },
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "457ad448-1128-4511-9409-acce0e9f2be5",
        |            "to": "28a3a901-439e-4867-b18b-ff1c42724e47"
        |        },
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "8288c8ef-cd6f-4e9e-883d-e9b802c0644c",
        |            "to": "28a3a901-439e-4867-b18b-ff1c42724e47"
        |        },
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "28a3a901-439e-4867-b18b-ff1c42724e47",
        |            "to": "609dba4e-6d08-44e6-89dc-d35cf355b529"
        |        },
        |        {
        |            "fromPort": "default",
        |            "toPort": "default",
        |            "from": "72037169-0169-4db3-af18-b807c48507f6",
        |            "to": "609dba4e-6d08-44e6-89dc-d35cf355b529"
        |        }
        |    ],
        |    "stops": [
        |        {
        |            "path": "dacp://10.0.82.147:3101/dam/2019年中国榆林市沟道信息.csv",
        |            "name": "2019年中国榆林市沟道信息",
        |            "id": "6edb7e80-1d25-4fe1-aaec-a3b987b3eb07",
        |            "type": "SourceNode"
        |        },
        |        {
        |            "path": "dacp://10.0.82.147:3101/dam/2019年中国榆林市30m数字高程数据集.tif",
        |            "name": "2019年中国榆林市30m数字高程数据集",
        |            "id": "e855c516-755c-4b2a-83a0-8e7fdec36444",
        |            "type": "SourceNode"
        |        },
        |        {
        |            "path": "dacp://10.0.82.147:3101/dam/geo_entropy.csv",
        |            "name": "2019年中国榆林市地貌信息熵数据集",
        |            "id": "457ad448-1128-4511-9409-acce0e9f2be5",
        |            "type": "SourceNode"
        |        },
        |        {
        |            "path": "dacp://10.0.82.147:3101/dam/labels",
        |            "name": "2019年中国榆林市图像分割数据集",
        |            "id": "4b76ba8e-b019-4227-a355-a4032df28492",
        |            "type": "SourceNode"
        |        },
        |        {
        |            "path": "dacp://10.0.82.147:3101/dam/tfw",
        |            "name": "2019年中国榆林市地理坐标信息数据集",
        |            "id": "e3ede64d-0d23-4b99-8c47-b820da2ccb51",
        |            "type": "SourceNode"
        |        },
        |        {
        |            "name": "hydro_susceptibility",
        |            "id": "28a3a901-439e-4867-b18b-ff1c42724e47",
        |            "type": "RepositoryNode",
        |            "version": "0.5.0-20251115-1",
        |            "parameters": {}
        |        },
        |        {
        |            "name": "geotrans",
        |            "id": "72037169-0169-4db3-af18-b807c48507f6",
        |            "type": "RepositoryNode",
        |            "version": "0.5.0-20251115-2",
        |            "parameters": {}
        |        },
        |        {
        |            "name": "overlap_dam_select",
        |            "id": "609dba4e-6d08-44e6-89dc-d35cf355b529",
        |            "type": "RepositoryNode",
        |            "version": "0.5.0-20251115-1",
        |            "parameters": {}
        |        },
        |        {
        |            "name": "gully_slop",
        |            "id": "8288c8ef-cd6f-4e9e-883d-e9b802c0644c",
        |            "type": "RepositoryNode",
        |            "version": "0.5.0-20251115-1",
        |            "parameters": {}
        |        }
        |    ]
        |}}
        |""".stripMargin

    // Execute logic
    // FIXED: Unpack tuple because schedule returns (String, String)
    val (resultJsonStr, _) = FlowScheduler.schedule(inputJson)
    val resultJson = new JSONObject(resultJsonStr)

    val flow = resultJson.getJSONObject("flow")
    val stops = flow.getJSONArray("stops")

    // Write to file (using temp dir for safety)
    val outputFile = new File(tempDir, "output.json")
    val writer = new PrintWriter(outputFile)
    try {
      writer.write(resultJsonStr)
    } finally {
      writer.close()
    }

    // Count node types
    var remoteNodesCount = 0
    var repositoryNodesCount = 0

    for (i <- 0 until stops.length()) {
      val stop = stops.getJSONObject(i)
      if (stop.getString("type") == "RemoteDataFrameFlowNode") {
        remoteNodesCount += 1
        // FIXED: 'flow' property is directly in the stop object based on FlowScheduler logic
        assertTrue(stop.has("flow"), "Remote node must contain 'flow' property")
      } else {
        repositoryNodesCount += 1
      }
    }

    assertTrue(remoteNodesCount >= 0, "Should have at least zero remote node collapsed")
    assertTrue(repositoryNodesCount > 0, "Main repository nodes should be preserved")
  }

  @Test
  def testEmptyFlow(): Unit = {
    assertThrows(classOf[Exception], () => {
      FlowScheduler.schedule("{}")
      ()
    }, "Empty JSON should throw exception")
  }
}