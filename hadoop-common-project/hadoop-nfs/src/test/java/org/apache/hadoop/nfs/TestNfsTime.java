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
package org.apache.hadoop.nfs;

import org.junit.Assert;

import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.oncrpc.XDR;
import org.junit.Test;

public class TestNfsTime {
  @Test
  public void testConstructor() {
    NfsTime nfstime = new NfsTime(1001);
    Assert.assertEquals(1, nfstime.getSeconds());
    Assert.assertEquals(1000000, nfstime.getNseconds());
  }
  
  @Test
  public void testSerializeDeserialize() {
    // Serialize NfsTime
    NfsTime t1 = new NfsTime(1001);
    XDR xdr = new XDR();
    t1.serialize(xdr);
    
    // Deserialize it back
    NfsTime t2 = NfsTime.deserialize(xdr.asReadOnlyWrap());
    
    // Ensure the NfsTimes are equal
    Assert.assertEquals(t1, t2);
  }
}
