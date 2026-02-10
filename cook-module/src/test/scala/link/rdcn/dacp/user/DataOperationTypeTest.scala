package link.rdcn.dacp.user

import link.rdcn.dacp.user.DataOperationType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class DataOperationTypeTest {

  @Test
  def testFromString_ExactMatch(): Unit = {
    val expected = Some(DataOperationType.Map)
    val actual = DataOperationType.fromString("Map")
    assertEquals(expected, actual, "fromString('Map') should return Some(DataOperationType.Map)")
  }

  @Test
  def testFromString_IgnoreCase(): Unit = {
    // Test Lowercase
    val expectedLower = Some(DataOperationType.Filter)
    val actualLower = DataOperationType.fromString("filter")
    assertEquals(expectedLower, actualLower, "fromString('filter') should be case-insensitive and return Some(DataOperationType.Filter)")

    // Test Uppercase
    val expectedUpper = Some(DataOperationType.Join)
    val actualUpper = DataOperationType.fromString("JOIN")
    assertEquals(expectedUpper, actualUpper, "fromString('JOIN') should be case-insensitive and return Some(DataOperationType.Join)")

    // Test Mixed Case
    val expectedMixed = Some(DataOperationType.Select)
    val actualMixed = DataOperationType.fromString("sElEcT")
    assertEquals(expectedMixed, actualMixed, "fromString('sElEcT') should be case-insensitive and return Some(DataOperationType.Select)")
  }

  @Test
  def testFromString_NotFound(): Unit = {
    val expected = None
    val actual = DataOperationType.fromString("NonExistentType")
    assertEquals(expected, actual, "fromString('NonExistentType') should return None")
  }

  @Test
  def testFromString_EmptyString(): Unit = {
    val expected = None
    val actual = DataOperationType.fromString("")
    assertEquals(expected, actual, "fromString('') should return None")
  }

  @Test
  def testValuesList_ContainsAllTypes(): Unit = {
    val values = DataOperationType.values

    // Verify count
    assertEquals(7, values.length, "values sequence should contain 7 elements")

    // Verify all members exist
    assertTrue(values.contains(DataOperationType.Map), "values sequence should contain Map")
    assertTrue(values.contains(DataOperationType.Filter), "values sequence should contain Filter")
    assertTrue(values.contains(DataOperationType.Select), "values sequence should contain Select")
    assertTrue(values.contains(DataOperationType.Reduce), "values sequence should contain Reduce")
    assertTrue(values.contains(DataOperationType.Join), "values sequence should contain Join")
    assertTrue(values.contains(DataOperationType.GroupBy), "values sequence should contain GroupBy")
    assertTrue(values.contains(DataOperationType.Sort), "values sequence should contain Sort")
  }
}