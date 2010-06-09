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

package org.apache.hadoop.fs;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.apache.hadoop.fs.permission.FsPermission;

public class TestFileStatus {

  @Test
  public void testFileStatusNullPath() throws Exception {

    new FileStatus();
    new FileStatus(1,false,3,3,4, new Path("/"));  new FileStatus(1,false,5,3,4,5,null,null,null,new Path("/a/b"));
    new FileStatus(1,false,5,3,4,5,null,null,null,null,new Path("/a/b"));
    new FileStatus(1,false,5,3,4,5,new FsPermission((short) 7),
            "bla","bla",new Path ("/b/c"), new Path("/a/b"));

    boolean error = false;
    try{
        new FileStatus(1,false,3,3,4, null);
    } catch (IllegalArgumentException exc) {
        error = true;
    }

    assertTrue("Accepted null path", error);

    try {
      new FileStatus(1,false,5,3,4,5,null,null,null,null);
    } catch (IllegalArgumentException exc) {
      error = true;
    }

    assertTrue("Accepted null path",error);

    try {
      new FileStatus(1,false,5,3,4,5,null,null,null,null,null);
    } catch (IllegalArgumentException exc) {
      error = true;
    }

    assertTrue("Accepted null path",error);

    try {
      new FileStatus(1,false,5,3,4,5,new FsPermission((short) 7),
          "bla","bla",new Path ("/b/c"), null);
    } catch (IllegalArgumentException exc) {
      error = true;
    }

    assertTrue("Accepted null path",error);

  }
}
