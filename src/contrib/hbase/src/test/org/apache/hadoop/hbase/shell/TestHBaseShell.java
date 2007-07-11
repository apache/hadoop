/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.shell;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HClient;
import org.apache.hadoop.hbase.shell.generated.ParseException;
import org.apache.hadoop.hbase.shell.generated.Parser;

public class TestHBaseShell extends TestCase {
  public void testParse() {
    String queryString1 = "SELECT test_table WHERE row='row_key' and " +
      "column='column_key';";
    new Parser(queryString1);
    
  }
}
