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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.util.StringUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * DAO object to display node allocation tags.
 */
@XmlRootElement(name = "allocationTagsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class AllocationTagsInfo {

  private ArrayList<AllocationTagInfo> allocationTagInfo;

  public AllocationTagsInfo() {
    allocationTagInfo = new ArrayList<>();
  }

  public void addAllocationTag(AllocationTagInfo info) {
    allocationTagInfo.add(info);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    Iterator<AllocationTagInfo> it = allocationTagInfo.iterator();
    while (it.hasNext()) {
      AllocationTagInfo current = it.next();
      sb.append(current.toString());
      if (it.hasNext()) {
        sb.append(StringUtils.COMMA);
      }
    }
    return sb.toString();
  }
}
