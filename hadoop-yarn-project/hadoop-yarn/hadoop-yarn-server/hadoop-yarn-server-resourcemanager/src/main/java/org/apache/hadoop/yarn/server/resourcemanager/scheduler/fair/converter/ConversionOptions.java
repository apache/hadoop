/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.slf4j.Logger;

public class ConversionOptions {
  private DryRunResultHolder dryRunResultHolder;
  private boolean dryRun;
  private boolean noTerminalRuleCheck;
  private boolean enableAsyncScheduler;

  public ConversionOptions(DryRunResultHolder dryRunResultHolder,
      boolean dryRun) {
    this.dryRunResultHolder = dryRunResultHolder;
    this.dryRun = dryRun;
  }

  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  public void setNoTerminalRuleCheck(boolean ruleTerminalCheck) {
    this.noTerminalRuleCheck = ruleTerminalCheck;
  }

  public boolean isNoRuleTerminalCheck() {
    return noTerminalRuleCheck;
  }

  public void setEnableAsyncScheduler(boolean enableAsyncScheduler) {
    this.enableAsyncScheduler = enableAsyncScheduler;
  }

  public boolean isEnableAsyncScheduler() {
    return enableAsyncScheduler;
  }

  public void handleWarning(String msg, Logger log) {
    if (dryRun) {
      dryRunResultHolder.addDryRunWarning(msg);
    } else {
      log.warn(msg);
    }
  }

  public void handleError(String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      throw new UnsupportedPropertyException(msg);
    }
  }

  public void handleConversionError(String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      throw new ConversionException(msg);
    }
  }

  public void handlePreconditionError(String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      throw new PreconditionException(msg);
    }
  }

  public void handleVerificationFailure(Throwable e, String msg) {
    FSConfigToCSConfigArgumentHandler.logAndStdErr(e, msg);
    if (dryRun) {
      dryRunResultHolder.setVerificationFailed();
    }
  }

  public void handleParsingFinished() {
    if (dryRun) {
      dryRunResultHolder.printDryRunResults();
    }
  }

  public void handleGenericException(Exception e, String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      FSConfigToCSConfigArgumentHandler.logAndStdErr(e, msg);
    }
  }
}
