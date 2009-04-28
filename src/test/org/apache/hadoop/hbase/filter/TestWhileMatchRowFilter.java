/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;


/**
 * Tests for the while-match filter
 */
public class TestWhileMatchRowFilter extends TestCase {

  WhileMatchRowFilter wmStopRowFilter;
  WhileMatchRowFilter wmRegExpRowFilter;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    wmStopRowFilter = new WhileMatchRowFilter(new StopRowFilter(
        Bytes.toBytes("s")));
    wmRegExpRowFilter = new WhileMatchRowFilter(new RegExpRowFilter(
    ".*regex.*"));
  }
  
  /**
   * Tests while match stop row
   * @throws Exception
   */
  public void testWhileMatchStopRow() throws Exception {
    whileMatchStopRowTests(wmStopRowFilter);
  }
  
  /**
   * Tests while match regex
   * @throws Exception
   */
  public void testWhileMatchRegExp() throws Exception {
    whileMatchRegExpTests(wmRegExpRowFilter);
  }
  
  /**
   * Tests serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    // Decompose wmRegExpRowFilter to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    wmRegExpRowFilter.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();
    
    // Recompose wmRegExpRowFilter.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    WhileMatchRowFilter newFilter = new WhileMatchRowFilter();
    newFilter.readFields(in);

    // Ensure the serialization preserved the filter by running a full test.
    whileMatchRegExpTests(newFilter);
  }
  
  private void whileMatchStopRowTests(WhileMatchRowFilter filter) throws 
    Exception {
    RowFilterInterface innerFilter = filter.getInternalFilter();
    String toTest;
    
    // Test cases that should pass the row
    toTest = "apples";
    assertFalse("filter: '" + toTest + "'", filter.filterRowKey(Bytes.toBytes(toTest)));
    byte [] toTestBytes = Bytes.toBytes(toTest);
    assertFalse("innerFilter: '" + toTest + "'",
      innerFilter.filterRowKey(toTestBytes, 0, toTestBytes.length));

    // Test cases that should fail the row
    toTest = "tuna";
    toTestBytes = Bytes.toBytes(toTest);
    assertTrue("filter: '" + toTest + "'", filter.filterRowKey(toTestBytes));
    assertTrue("innerFilter: '" + toTest + "'",
      innerFilter.filterRowKey(toTestBytes, 0, toTestBytes.length));
    
    // The difference in switch
    assertTrue("filter: filterAllRemaining", filter.filterAllRemaining());
    assertFalse("innerFilter: filterAllRemaining pre-reset", 
      innerFilter.filterAllRemaining());
    
    // Test resetting
    filter.reset();
    assertFalse("filter: filterAllRemaining post-reset", 
        filter.filterAllRemaining());
    
    // Test filterNotNull for functionality only (no switch-cases)
    assertFalse("filter: filterNotNull", filter.filterRow((List<KeyValue>)null));
  }
  
  private void whileMatchRegExpTests(WhileMatchRowFilter filter) throws 
    Exception {
    RowFilterInterface innerFilter = filter.getInternalFilter();
    String toTest;
    
    // Test cases that should pass the row
    toTest = "regex_match";
    byte [] toTestBytes = Bytes.toBytes(toTest);
    assertFalse("filter: '" + toTest + "'", filter.filterRowKey(Bytes.toBytes(toTest)));
    assertFalse("innerFilter: '" + toTest + "'",
      innerFilter.filterRowKey(toTestBytes, 0, toTestBytes.length));
    
    // Test cases that should fail the row
    toTest = "not_a_match";
    toTestBytes = Bytes.toBytes(toTest);
    assertTrue("filter: '" + toTest + "'", filter.filterRowKey(Bytes.toBytes(toTest)));
    assertTrue("innerFilter: '" + toTest + "'",
      innerFilter.filterRowKey(toTestBytes, 0, toTestBytes.length));
    
    // The difference in switch
    assertTrue("filter: filterAllRemaining", filter.filterAllRemaining());
    assertFalse("innerFilter: filterAllRemaining pre-reset", 
      innerFilter.filterAllRemaining());
    
    // Test resetting
    filter.reset();
    assertFalse("filter: filterAllRemaining post-reset", 
        filter.filterAllRemaining());
    
    // Test filter(Text, Text, byte[]) for functionality only (no switch-cases)
    toTest = "asdf_regex_hjkl";
    toTestBytes = Bytes.toBytes(toTest);
    assertFalse("filter: '" + toTest + "'",
      filter.filterColumn(toTestBytes, 0, toTestBytes.length, 
      null, 0, 0, null, 0, 0));
  }
}
