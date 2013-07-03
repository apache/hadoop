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
package org.apache.hadoop.oncrpc;

/**
 * AUTH_SYS as defined in RFC 1831
 */
public class RpcAuthSys {
  private final int uid;
  private final int gid;

  public RpcAuthSys(int uid, int gid) {
    this.uid = uid;
    this.gid = gid;
  }
  
  public static RpcAuthSys from(byte[] credentials) {
    XDR sys = new XDR(credentials);
    sys.skip(4); // Stamp
    sys.skipVariableOpaque(); // Machine name
    return new RpcAuthSys(sys.readInt(), sys.readInt());
  }
  
  public int getUid() {
    return uid;
  }

  public int getGid() {
    return gid;
  }

  @Override
  public String toString() {
    return "(AuthSys: uid=" + uid + " gid=" + gid + ")";
  }
}
