/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.server.SCMChillModeManager.ChillModeRestrictedOps;

/**
 * Chill mode pre-check for SCM operations.
 * */
public class ChillModePrecheck implements Precheck<ScmOps> {

  private AtomicBoolean inChillMode = new AtomicBoolean(true);
  public static final String PRECHECK_TYPE = "ChillModePrecheck";

  public boolean check(ScmOps op) throws SCMException {
    if (inChillMode.get() && ChillModeRestrictedOps
        .isRestrictedInChillMode(op)) {
      throw new SCMException("ChillModePrecheck failed for " + op,
          ResultCodes.CHILL_MODE_EXCEPTION);
    }
    return inChillMode.get();
  }

  @Override
  public String type() {
    return PRECHECK_TYPE;
  }

  public boolean isInChillMode() {
    return inChillMode.get();
  }

  public void setInChillMode(boolean inChillMode) {
    this.inChillMode.set(inChillMode);
  }
}
