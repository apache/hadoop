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
package org.apache.hadoop.hdds.scm.safemode;

import java.util.EnumSet;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;

/**
 * Operations restricted in SCM safe mode.
 */
public final class SafeModeRestrictedOps {
  private static EnumSet restrictedOps = EnumSet.noneOf(ScmOps.class);

  private SafeModeRestrictedOps() {
  }

  static {
    restrictedOps.add(ScmOps.allocateBlock);
    restrictedOps.add(ScmOps.allocateContainer);
  }

  public static boolean isRestrictedInSafeMode(ScmOps opName) {
    return restrictedOps.contains(opName);
  }
}