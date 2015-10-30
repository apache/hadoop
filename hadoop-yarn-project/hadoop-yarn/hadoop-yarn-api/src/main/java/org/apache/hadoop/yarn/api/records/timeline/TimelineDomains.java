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

package org.apache.hadoop.yarn.api.records.timeline;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * The class that hosts a list of timeline domains.
 */
@XmlRootElement(name = "domains")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineDomains {

  private List<TimelineDomain> domains = new ArrayList<TimelineDomain>();

  public TimelineDomains() {
  }

  /**
   * Get a list of domains
   * 
   * @return a list of domains
   */
  @XmlElement(name = "domains")
  public List<TimelineDomain> getDomains() {
    return domains;
  }

  /**
   * Add a single domain into the existing domain list
   * 
   * @param domain
   *          a single domain
   */
  public void addDomain(TimelineDomain domain) {
    domains.add(domain);
  }

  /**
   * All a list of domains into the existing domain list
   * 
   * @param domains
   *          a list of domains
   */
  public void addDomains(List<TimelineDomain> domains) {
    this.domains.addAll(domains);
  }

  /**
   * Set the domain list to the given list of domains
   * 
   * @param domains
   *          a list of domains
   */
  public void setDomains(List<TimelineDomain> domains) {
    this.domains = domains;
  }

}
