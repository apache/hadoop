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

package org.apache.hadoop.yarn.webapp.dao;

import java.util.HashMap;
import java.util.Map;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Information for adding or updating a queue to scheduler configuration
 * for this queue.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueConfigInfo {

  @XmlElement(name = "queue-name")
  private String queue;

  private HashMap<String, String> params = new HashMap<>();

  public QueueConfigInfo() { }

  public QueueConfigInfo(String queue, Map<String, String> params) {
    this.queue = queue;
    this.params = new HashMap<>(params);
  }

  public String getQueue() {
    return this.queue;
  }

  public HashMap<String, String> getParams() {
    return this.params;
  }

}
