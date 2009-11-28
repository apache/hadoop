/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.sqoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;

/**
 * Test that each of the different SQL Column types that we support
 * can, in fact, be imported into HDFS. Test that the writable
 * that we expect to work, does.
 *
 * This requires testing:
 * - That we can pull from the database into HDFS:
 *    readFields(ResultSet), toString()
 * - That we can pull from mapper to reducer:
 *    write(DataOutput), readFields(DataInput)
 * - And optionally, that we can push to the database:
 *    write(PreparedStatement)
 */
public class TestColumnTypes extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(TestColumnTypes.class.getName());

  /**
   * Do a full verification test on the singleton value of a given type.
   * @param colType  The SQL type to instantiate the column
   * @param insertVal The SQL text to insert a value into the database
   * @param returnVal The string representation of the value as extracted from the db
   */
  private void verifyType(String colType, String insertVal, String returnVal) {
    verifyType(colType, insertVal, returnVal, returnVal);
  }

  /**
   * Do a full verification test on the singleton value of a given type.
   * @param colType  The SQL type to instantiate the column
   * @param insertVal The SQL text to insert a value into the database
   * @param returnVal The string representation of the value as extracted from the db
   * @param seqFileVal The string representation of the value as extracted through
   *        the DBInputFormat, serialized, and injected into a SequenceFile and put
   *        through toString(). This may be slightly different than what ResultSet.getString()
   *        returns, which is used by returnVal.
   */
  private void verifyType(String colType, String insertVal, String returnVal, String seqFileVal) {
    createTableForColType(colType, insertVal);
    verifyReadback(1, returnVal);
    verifyImport(seqFileVal, null);
  }

  static final String STRING_VAL_IN = "'this is a short string'";
  static final String STRING_VAL_OUT = "this is a short string";

  @Test
  public void testStringCol1() {
    verifyType("VARCHAR(32)", STRING_VAL_IN, STRING_VAL_OUT);
  }

  @Test
  public void testStringCol2() {
    verifyType("CHAR(32)", STRING_VAL_IN, STRING_VAL_OUT);
  }

  @Test
  public void testEmptyStringCol() {
    verifyType("VARCHAR(32)", "''", "");
  }

  @Test
  public void testNullStringCol() {
    verifyType("VARCHAR(32)", "NULL", null);
  }

  @Test
  public void testInt() {
    verifyType("INTEGER", "42", "42");
  }

  @Test
  public void testNullInt() {
    verifyType("INTEGER", "NULL", null);
  }

  @Test
  public void testBit1() {
    verifyType("BIT", "1", "true");
  }

  @Test
  public void testBit2() {
    verifyType("BIT", "0", "false");
  }

  @Test
  public void testBit3() {
    verifyType("BIT", "false", "false");
  }

  @Test
  public void testTinyInt1() {
    verifyType("TINYINT", "0", "0");
  }

  @Test
  public void testTinyInt2() {
    verifyType("TINYINT", "42", "42");
  }

  @Test
  public void testSmallInt1() {
    verifyType("SMALLINT", "-1024", "-1024");
  }

  @Test
  public void testSmallInt2() {
    verifyType("SMALLINT", "2048", "2048");
  }

  @Test
  public void testBigInt1() {
    verifyType("BIGINT", "10000000000", "10000000000");
  }

  @Test
  public void testReal1() {
    verifyType("REAL", "256", "256.0");
  }

  @Test
  public void testReal2() {
    verifyType("REAL", "256.45", "256.45");
  }

  @Test
  public void testFloat1() {
    verifyType("FLOAT", "256", "256.0");
  }

  @Test
  public void testFloat2() {
    verifyType("FLOAT", "256.45", "256.45");
  }

  @Test
  public void testDouble1() {
    verifyType("DOUBLE", "-256", "-256.0");
  }

  @Test
  public void testDouble2() {
    verifyType("DOUBLE", "256.45", "256.45");
  }

  @Test
  public void testDate1() {
    verifyType("DATE", "'2009-1-12'", "2009-01-12");
  }

  @Test
  public void testDate2() {
    verifyType("DATE", "'2009-01-12'", "2009-01-12");
  }

  @Test
  public void testDate3() {
    verifyType("DATE", "'2009-04-24'", "2009-04-24");
  }

  @Test
  public void testTime1() {
    verifyType("TIME", "'12:24:00'", "12:24:00");
  }

  @Test
  public void testTime2() {
    verifyType("TIME", "'06:24:00'", "06:24:00");
  }

  @Test
  public void testTime3() {
    verifyType("TIME", "'6:24:00'", "06:24:00");
  }

  @Test
  public void testTime4() {
    verifyType("TIME", "'18:24:00'", "18:24:00");
  }

  @Test
  public void testTimestamp1() {
    verifyType("TIMESTAMP", "'2009-04-24 18:24:00'",
        "2009-04-24 18:24:00.000000000",
        "2009-04-24 18:24:00.0");
  }

  @Test
  public void testTimestamp2() {
    try {
    LOG.debug("Beginning testTimestamp2");
    verifyType("TIMESTAMP", "'2009-04-24 18:24:00.0002'",
        "2009-04-24 18:24:00.000200000",
        "2009-04-24 18:24:00.0002");
    } finally {
      LOG.debug("End testTimestamp2");
    }
  }

  @Test
  public void testTimestamp3() {
    try {
    LOG.debug("Beginning testTimestamp3");
    verifyType("TIMESTAMP", "null", null);
    } finally {
      LOG.debug("End testTimestamp3");
    }
  }

  @Test
  public void testNumeric1() {
    verifyType("NUMERIC", "1", "1");
  }

  @Test
  public void testNumeric2() {
    verifyType("NUMERIC", "-10", "-10");
  }

  @Test
  public void testNumeric3() {
    verifyType("NUMERIC", "3.14159", "3.14159");
  }

  @Test
  public void testNumeric4() {
    verifyType("NUMERIC", "30000000000000000000000000.14159", "30000000000000000000000000.14159");
  }

  @Test
  public void testNumeric5() {
    verifyType("NUMERIC", "999999999999999999999999999999.14159", "999999999999999999999999999999.14159");
  }

  @Test
  public void testNumeric6() {
    verifyType("NUMERIC", "-999999999999999999999999999999.14159", "-999999999999999999999999999999.14159");
  }

  @Test
  public void testDecimal1() {
    verifyType("DECIMAL", "1", "1");
  }

  @Test
  public void testDecimal2() {
    verifyType("DECIMAL", "-10", "-10");
  }

  @Test
  public void testDecimal3() {
    verifyType("DECIMAL", "3.14159", "3.14159");
  }

  @Test
  public void testDecimal4() {
    verifyType("DECIMAL", "30000000000000000000000000.14159", "30000000000000000000000000.14159");
  }

  @Test
  public void testDecimal5() {
    verifyType("DECIMAL", "999999999999999999999999999999.14159", "999999999999999999999999999999.14159");
  }

  @Test
  public void testDecimal6() {
    verifyType("DECIMAL", "-999999999999999999999999999999.14159", "-999999999999999999999999999999.14159");
  }

  @Test
  public void testLongVarChar() {
    verifyType("LONGVARCHAR", "'this is a long varchar'", "this is a long varchar");
  }

}
