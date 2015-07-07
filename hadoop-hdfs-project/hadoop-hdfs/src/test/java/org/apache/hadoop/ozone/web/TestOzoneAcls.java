/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.web;

import org.apache.hadoop.ozone.web.request.OzoneAcl;
import org.junit.Test;

import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestOzoneAcls {

  @Test
  public void TestACLParse() {
    HashMap<String, Boolean> testMatrix;
    testMatrix = new HashMap<>();

    testMatrix.put("user:bilbo:r", Boolean.TRUE);
    testMatrix.put("user:bilbo:w", Boolean.TRUE);
    testMatrix.put("user:bilbo:rw", Boolean.TRUE);
    testMatrix.put("user:bilbo:wr", Boolean.TRUE);
    testMatrix.put("    user:bilbo:wr   ", Boolean.TRUE);


    // ACLs makes no judgement on the quality of
    // user names. it is for the userAuth interface
    // to determine if a user name is really a name
    testMatrix.put(" user:*:rw", Boolean.TRUE);
    testMatrix.put(" user:~!:rw", Boolean.TRUE);


    testMatrix.put("", Boolean.FALSE);
    testMatrix.put(null, Boolean.FALSE);
    testMatrix.put(" user:bilbo:", Boolean.FALSE);
    testMatrix.put(" user:bilbo:rx", Boolean.FALSE);
    testMatrix.put(" user:bilbo:mk", Boolean.FALSE);
    testMatrix.put(" user::rw", Boolean.FALSE);
    testMatrix.put("user11:bilbo:rw", Boolean.FALSE);
    testMatrix.put(" user:::rw", Boolean.FALSE);

    testMatrix.put(" group:hobbit:r", Boolean.TRUE);
    testMatrix.put(" group:hobbit:w", Boolean.TRUE);
    testMatrix.put(" group:hobbit:rw", Boolean.TRUE);
    testMatrix.put(" group:hobbit:wr", Boolean.TRUE);
    testMatrix.put(" group:*:rw", Boolean.TRUE);
    testMatrix.put(" group:~!:rw", Boolean.TRUE);

    testMatrix.put(" group:hobbit:", Boolean.FALSE);
    testMatrix.put(" group:hobbit:rx", Boolean.FALSE);
    testMatrix.put(" group:hobbit:mk", Boolean.FALSE);
    testMatrix.put(" group::", Boolean.FALSE);
    testMatrix.put(" group::rw", Boolean.FALSE);
    testMatrix.put(" group22:hobbit:", Boolean.FALSE);
    testMatrix.put(" group:::rw", Boolean.FALSE);

    testMatrix.put("JUNK group:hobbit:r", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:w", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:rw", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:wr", Boolean.FALSE);
    testMatrix.put("JUNK group:*:rw", Boolean.FALSE);
    testMatrix.put("JUNK group:~!:rw", Boolean.FALSE);

    testMatrix.put(" world::r", Boolean.TRUE);
    testMatrix.put(" world::w", Boolean.TRUE);
    testMatrix.put(" world::rw", Boolean.TRUE);
    testMatrix.put(" world::wr", Boolean.TRUE);

    testMatrix.put(" world:bilbo:w", Boolean.FALSE);
    testMatrix.put(" world:bilbo:rw", Boolean.FALSE);

    Set<String> keys = testMatrix.keySet();
    for (String key : keys) {
      if (testMatrix.get(key)) {
        OzoneAcl.parseAcl(key);
      } else {
        try {
          OzoneAcl.parseAcl(key);
          // should never get here since parseAcl will throw
          fail("An exception was expected but did not happen.");
        } catch (IllegalArgumentException e) {
          // nothing to do
        }
      }
    }
  }

  @Test
  public void TestACLValues() {
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    assertEquals(acl.getName(), "bilbo");
    assertEquals(acl.getRights(), OzoneAcl.OzoneACLRights.READ_WRITE);
    assertEquals(acl.getType(), OzoneAcl.OzoneACLType.USER);

    acl = OzoneAcl.parseAcl("user:bilbo:wr");
    assertEquals(acl.getName(), "bilbo");
    assertEquals(acl.getRights(), OzoneAcl.OzoneACLRights.READ_WRITE);
    assertEquals(acl.getType(), OzoneAcl.OzoneACLType.USER);

    acl = OzoneAcl.parseAcl("user:bilbo:r");
    assertEquals(acl.getName(), "bilbo");
    assertEquals(acl.getRights(), OzoneAcl.OzoneACLRights.READ);
    assertEquals(acl.getType(), OzoneAcl.OzoneACLType.USER);

    acl = OzoneAcl.parseAcl("user:bilbo:w");
    assertEquals(acl.getName(), "bilbo");
    assertEquals(acl.getRights(), OzoneAcl.OzoneACLRights.WRITE);
    assertEquals(acl.getType(), OzoneAcl.OzoneACLType.USER);

    acl = OzoneAcl.parseAcl("group:hobbit:wr");
    assertEquals(acl.getName(), "hobbit");
    assertEquals(acl.getRights(), OzoneAcl.OzoneACLRights.READ_WRITE);
    assertEquals(acl.getType(), OzoneAcl.OzoneACLType.GROUP);

    acl = OzoneAcl.parseAcl("world::wr");
    assertEquals(acl.getName(), "");
    assertEquals(acl.getRights(), OzoneAcl.OzoneACLRights.READ_WRITE);
    assertEquals(acl.getType(), OzoneAcl.OzoneACLType.WORLD);
  }

}
