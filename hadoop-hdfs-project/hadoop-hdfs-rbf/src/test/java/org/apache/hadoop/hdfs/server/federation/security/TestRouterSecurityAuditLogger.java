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

package org.apache.hadoop.hdfs.server.federation.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.security.RouterSecurityAuditLogger;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestRouterSecurityAuditLogger {

  @Test
  public void testRouterSecurityAuditLog() throws IOException {
    RouterSecurityAuditLogger auditLogger =
        new RouterSecurityAuditLogger(new Configuration());
    String fLog1 = auditLogger.creatAuditLog(false, "foo_user",
        Server.getRemoteIp(), "getDelegationToken", CallerContext.getCurrent(),
        "tokenId-123");
    String expLog1 =
        "allowed=false\tugi=foo_user\tip=null\tcmd=getDelegationToken\t" +
            "\ttoeknId=tokenId-123\tproto=null";
    assertEquals(expLog1, fLog1);

    String fLog2 = auditLogger.creatAuditLog(true, "foo2_user",
        Server.getRemoteIp(), "renewDelegationToken", CallerContext.getCurrent(),
        "tokenId-456");
    String expLog2 =
        "allowed=true\tugi=foo2_user\tip=null\tcmd=renewDelegationToken\t" +
            "\ttoeknId=tokenId-456\tproto=null";
    assertEquals(expLog2, fLog2);
  }
}