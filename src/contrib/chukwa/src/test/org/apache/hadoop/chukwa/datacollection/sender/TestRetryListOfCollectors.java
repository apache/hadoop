/*
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
package org.apache.hadoop.chukwa.datacollection.sender;

import junit.framework.TestCase;
import java.util.*;

import org.apache.hadoop.chukwa.datacollection.sender.RetryListOfCollectors;

public class TestRetryListOfCollectors extends TestCase {

  public void testRetryList()
  {
    List<String> hosts = new ArrayList<String>();
    hosts.add("host1");
    hosts.add("host2");
    hosts.add("host3");
    hosts.add("host4");
    RetryListOfCollectors rloc = new RetryListOfCollectors(hosts, 2000);
    assertEquals(hosts.size(), rloc.total());
    
    for(int i = 0; i < hosts.size(); ++i) {
      assertTrue(rloc.hasNext());
      String s =  rloc.next();
      assertTrue(s != null);
      System.out.println(s);
    }
    
    if(rloc.hasNext()) {
      String s = rloc.next();
      System.out.println("saw unexpected collector " + s);
      fail();
    }
  
  }

}
