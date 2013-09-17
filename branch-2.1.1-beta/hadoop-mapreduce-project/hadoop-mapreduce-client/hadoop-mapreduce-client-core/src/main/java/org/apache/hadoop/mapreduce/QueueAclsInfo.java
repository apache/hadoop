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
package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringInterner;

/**
 *  Class to encapsulate Queue ACLs for a particular
 *  user.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QueueAclsInfo implements Writable {

  private String queueName;
  private String[] operations;
  /**
   * Default constructor for QueueAclsInfo.
   * 
   */
  public QueueAclsInfo() {
    
  }

  /**
   * Construct a new QueueAclsInfo object using the queue name and the
   * queue operations array
   * 
   * @param queueName Name of the job queue
   * @param operations
   */
  public QueueAclsInfo(String queueName, String[] operations) {
    this.queueName = queueName;
    this.operations = operations;    
  }

  /**
   * Get queue name.
   * 
   * @return name
   */
  public String getQueueName() {
    return queueName;
  }

  protected void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  /**
   * Get opearations allowed on queue.
   * 
   * @return array of String
   */
  public String[] getOperations() {
    return operations;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    queueName = StringInterner.weakIntern(Text.readString(in));
    operations = WritableUtils.readStringArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, queueName);
    WritableUtils.writeStringArray(out, operations);
  }
}
