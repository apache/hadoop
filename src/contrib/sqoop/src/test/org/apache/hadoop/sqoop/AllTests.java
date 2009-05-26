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

import org.apache.hadoop.sqoop.manager.TestHsqldbManager;
import org.apache.hadoop.sqoop.manager.TestSqlManager;
import org.apache.hadoop.sqoop.orm.TestClassWriter;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * All tests for Sqoop (org.apache.hadoop.sqoop)
 *
 * 
 */
public final class AllTests  {

  private AllTests() { }

  public static Test suite() {
    TestSuite suite = new TestSuite("Tests for org.apache.hadoop.sqoop");

    suite.addTestSuite(TestAllTables.class);
    suite.addTestSuite(TestHsqldbManager.class);
    suite.addTestSuite(TestSqlManager.class);
    suite.addTestSuite(TestClassWriter.class);
    suite.addTestSuite(TestColumnTypes.class);
    suite.addTestSuite(TestMultiCols.class);
    suite.addTestSuite(TestOrderBy.class);

    return suite;
  }

}

