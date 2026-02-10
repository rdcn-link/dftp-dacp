package link.rdcn.dacp.optree.fifo

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}
import link.rdcn.dacp.optree.fifo.DockerExecute
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.condition.{DisabledOnOs, OS}
import org.junit.jupiter.api.{AfterEach, Assumptions, BeforeEach, Test}

import java.io.IOException

class DockerExecuteTest {

  private val CONTAINER_NAME = "jyg-container"
  private var dockerClient: DockerClient = _

  @BeforeEach
  def setUp(): Unit = {
    Assumptions.assumeTrue(isDockerAvailable(), "Docker not available, skipping test")
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    dockerClient = DockerClientBuilder.getInstance(config).build()
  }

  private def isDockerAvailable(): Boolean = {
    try {
      new ProcessBuilder("docker", "info").start().waitFor() == 0
    } catch {
      case _: IOException => false
    }
  }

  @AfterEach
  def tearDown(): Unit = {
    if (dockerClient != null) dockerClient.close()
  }

  @Test
  @DisabledOnOs(value = Array(OS.WINDOWS), disabledReason = "mkfifo not available on Windows")
  def testNonInteractiveEchoExec(): Unit = {
    // Assuming container exists for this integration test
    if (!DockerExecute.isContainerRunning(CONTAINER_NAME)) return

    val command = Array("echo", "Hello")
    val output = DockerExecute.nonInteractiveExec(command, CONTAINER_NAME)
    assertTrue(output.contains("Hello"), "Output should contain echoed string")
  }
}