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
package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class is a simple struct to include both the taskFound information and
 * a possible preemption request coming from the AM.
 */
public class AMFeedback implements Writable {

  boolean taskFound;
  boolean preemption;

  public void setTaskFound(boolean t){
    taskFound=t;
  }

  public boolean getTaskFound(){
    return taskFound;
  }

  public void setPreemption(boolean preemption) {
    this.preemption=preemption;
  }

  public boolean getPreemption() {
    return preemption;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(taskFound);
    out.writeBoolean(preemption);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskFound = in.readBoolean();
    preemption = in.readBoolean();
  }

}
