/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.hadoop.mapred.lib.db;

import org.apache.hadoop.io.NullWritable;

import junit.framework.TestCase;

public class TestConstructQuery extends TestCase {
  public void testConstructQuery() {
    DBOutputFormat<DBWritable, NullWritable> format = new DBOutputFormat<DBWritable, NullWritable>();
    String expected = "INSERT INTO hadoop_output (id,name,value) VALUES (?,?,?);";
    String[] fieldNames = new String[] { "id", "name", "value" };
    String actual = format.constructQuery("hadoop_output", fieldNames);
    assertEquals(expected, actual);
    expected = "INSERT INTO hadoop_output VALUES (?,?,?);";
    fieldNames = new String[] { null, null, null };
    actual = format.constructQuery("hadoop_output", fieldNames);
    assertEquals(expected, actual);
  }
}
