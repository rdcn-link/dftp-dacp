/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:54
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.struct.ValueType._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class ValueTypeTest {

  @Test
  def testStandardTypes(): Unit = {
    assertEquals("Int", IntType.name)
    assertEquals("String", StringType.name)
    assertEquals("Boolean", BooleanType.name)
    assertEquals("Blob", BinaryType.name)
  }

  @Test
  def testIsNumeric(): Unit = {
    assertTrue(ValueType.isNumeric(IntType), "Int should be numeric")
    assertTrue(ValueType.isNumeric(LongType), "Long should be numeric")
    assertTrue(ValueType.isNumeric(FloatType), "Float should be numeric")
    assertTrue(ValueType.isNumeric(DoubleType), "Double should be numeric")

    assertFalse(ValueType.isNumeric(StringType), "String should not be numeric")
    assertFalse(ValueType.isNumeric(BinaryType), "Blob should not be numeric")
  }

  @Test
  def testFromName(): Unit = {
    assertEquals(Some(IntType), ValueType.fromName("Int"))
    assertEquals(Some(IntType), ValueType.fromName("int")) // Case insensitive
    assertEquals(Some(StringType), ValueType.fromName("String"))
    assertEquals(None, ValueType.fromName("InvalidType"))
  }
}