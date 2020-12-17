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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

public class DryRunResultHolder {
  private static final Logger LOG =
      LoggerFactory.getLogger(DryRunResultHolder.class);

  private Set<String> warnings;
  private Set<String> errors;
  private boolean verificationFailed;

  public DryRunResultHolder() {
    this.warnings = new HashSet<>();
    this.errors = new HashSet<>();
  }

  public void addDryRunWarning(String message) {
    warnings.add(message);
  }

  public void addDryRunError(String message) {
    errors.add(message);
  }

  public void setVerificationFailed() {
    verificationFailed = true;
  }

  public Set<String> getWarnings() {
    return ImmutableSet.copyOf(warnings);
  }

  public Set<String> getErrors() {
    return ImmutableSet.copyOf(errors);
  }

  public void printDryRunResults() {
    LOG.info("");
    LOG.info("Results of dry run:");
    LOG.info("");

    int noOfErrors = errors.size();
    int noOfWarnings = warnings.size();

    LOG.info("Number of errors: {}", noOfErrors);
    LOG.info("Number of warnings: {}", noOfWarnings);
    LOG.info("Verification result: {}",
        verificationFailed ? "FAILED" : "PASSED");

    if (noOfErrors > 0) {
      LOG.info("");
      LOG.info("List of errors:");
      errors.forEach(s -> LOG.info(s));
    }

    if (noOfWarnings > 0) {
      LOG.info("");
      LOG.info("List of warnings:");
      warnings.forEach(s -> LOG.info(s));
    }
  }
}
