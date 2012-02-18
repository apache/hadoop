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

/**
 *  Class to encapsulate Queue ACLs for a particular
 *  user.
 */
class QueueAclsInfo extends org.apache.hadoop.mapreduce.QueueAclsInfo {

  /**
   * Default constructor for QueueAclsInfo.
   * 
   */
  QueueAclsInfo() {
    super();
  }

  /**
   * Construct a new QueueAclsInfo object using the queue name and the
   * queue operations array
   * 
   * @param queueName Name of the job queue
   * @param queue operations
   * 
   */
  QueueAclsInfo(String queueName, String[] operations) {
    super(queueName, operations);
  }
  
  public static QueueAclsInfo downgrade(
      org.apache.hadoop.mapreduce.QueueAclsInfo acl) {
    return new QueueAclsInfo(acl.getQueueName(), acl.getOperations());
  }
}
