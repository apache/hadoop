/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.master.AssignmentManager.TimeOutOperationType;

/**
 * A callable object that invokes the corresponding action that needs to be
 * taken when timeout thread deducts a region was in tranisition for a long
 * time.  Implementing as future callable we are able to act on the timeout
 * asynchronoulsy
 *
 */
public class TimeOutManagerCallable implements Callable<Object> {

  private AssignmentManager assignmentManager;

  private HRegionInfo hri;

  private TimeOutOperationType operation;

  public TimeOutManagerCallable(AssignmentManager assignmentManager,
      HRegionInfo hri, TimeOutOperationType operation) {
    this.assignmentManager = assignmentManager;
    this.hri = hri;
    this.operation = operation;
  }

  @Override
  public Object call() throws Exception {
    if (TimeOutOperationType.ASSIGN.equals(operation)) {
      assignmentManager.assign(hri, true, true, true);
    } else {
      assignmentManager.unassign(hri);
    }
    return null;
  }

}



