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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * <p>
 * This class contains the information about a timeline domain, which is used
 * to a user to host a number of timeline entities, isolating them from others'.
 * The user can also define the reader and writer users/groups for the the
 * domain, which is used to control the access to its entities.
 * </p>
 * 
 * <p>
 * The reader and writer users/groups pattern that the user can supply is the
 * same as what <code>AccessControlList</code> takes.
 * </p>
 * 
 */
@XmlRootElement(name = "domain")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineDomain {

  private String id;
  private String description;
  private String owner;
  private String readers;
  private String writers;
  private Long createdTime;
  private Long modifiedTime;

  public TimelineDomain() {
  }

  /**
   * Get the domain ID
   * 
   * @return the domain ID
   */
  @XmlElement(name = "id")
  public String getId() {
    return id;
  }

  /**
   * Set the domain ID
   * 
   * @param id the domain ID
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Get the domain description
   * 
   * @return the domain description
   */
  @XmlElement(name = "description")
  public String getDescription() {
    return description;
  }

  /**
   * Set the domain description
   * 
   * @param description the domain description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Get the domain owner
   * 
   * @return the domain owner
   */
  @XmlElement(name = "owner")
  public String getOwner() {
    return owner;
  }

  /**
   * Set the domain owner. The user doesn't need to set it, which will
   * automatically set to the user who puts the domain.
   * 
   * @param owner the domain owner
   */
  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * Get the reader (and/or reader group) list string
   * 
   * @return the reader (and/or reader group) list string
   */
  @XmlElement(name = "readers")
  public String getReaders() {
    return readers;
  }

  /**
   * Set the reader (and/or reader group) list string
   * 
   * @param readers the reader (and/or reader group) list string
   */
  public void setReaders(String readers) {
    this.readers = readers;
  }

  /**
   * Get the writer (and/or writer group) list string
   * 
   * @return the writer (and/or writer group) list string
   */
  @XmlElement(name = "writers")
  public String getWriters() {
    return writers;
  }

  /**
   * Set the writer (and/or writer group) list string
   * 
   * @param writers the writer (and/or writer group) list string
   */
  public void setWriters(String writers) {
    this.writers = writers;
  }

  /**
   * Get the created time of the domain
   * 
   * @return the created time of the domain
   */
  @XmlElement(name = "createdtime")
  public Long getCreatedTime() {
    return createdTime;
  }

  /**
   * Set the created time of the domain
   * 
   * @param createdTime the created time of the domain
   */
  public void setCreatedTime(Long createdTime) {
    this.createdTime = createdTime;
  }

  /**
   * Get the modified time of the domain
   * 
   * @return the modified time of the domain
   */
  @XmlElement(name = "modifiedtime")
  public Long getModifiedTime() {
    return modifiedTime;
  }

  /**
   * Set the modified time of the domain
   * 
   * @param modifiedTime the modified time of the domain
   */
  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }

}
