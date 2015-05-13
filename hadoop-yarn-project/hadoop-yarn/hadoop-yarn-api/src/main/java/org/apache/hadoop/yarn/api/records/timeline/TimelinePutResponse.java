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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * A class that holds a list of put errors. This is the response returned when a
 * list of {@link TimelineEntity} objects is added to the timeline. If there are errors
 * in storing individual entity objects, they will be indicated in the list of
 * errors.
 */
@XmlRootElement(name = "response")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelinePutResponse {

  private List<TimelinePutError> errors = new ArrayList<TimelinePutError>();

  public TimelinePutResponse() {

  }

  /**
   * Get a list of {@link TimelinePutError} instances
   * 
   * @return a list of {@link TimelinePutError} instances
   */
  @XmlElement(name = "errors")
  public List<TimelinePutError> getErrors() {
    return errors;
  }

  /**
   * Add a single {@link TimelinePutError} instance into the existing list
   * 
   * @param error
   *          a single {@link TimelinePutError} instance
   */
  public void addError(TimelinePutError error) {
    errors.add(error);
  }

  /**
   * Add a list of {@link TimelinePutError} instances into the existing list
   * 
   * @param errors
   *          a list of {@link TimelinePutError} instances
   */
  public void addErrors(List<TimelinePutError> errors) {
    this.errors.addAll(errors);
  }

  /**
   * Set the list to the given list of {@link TimelinePutError} instances
   * 
   * @param errors
   *          a list of {@link TimelinePutError} instances
   */
  public void setErrors(List<TimelinePutError> errors) {
    this.errors.clear();
    this.errors.addAll(errors);
  }

  /**
   * A class that holds the error code for one entity.
   */
  @XmlRootElement(name = "error")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Evolving
  public static class TimelinePutError {

    /**
     * Error code returned when no start time can be found when putting an
     * entity. This occurs when the entity does not already exist in the store
     * and it is put with no start time or events specified.
     */
    public static final int NO_START_TIME = 1;
    /**
     * Error code returned if an IOException is encountered when putting an
     * entity.
     */
    public static final int IO_EXCEPTION = 2;

    /**
     * Error code returned if the user specifies the timeline system reserved
     * filter key
     */
    public static final int SYSTEM_FILTER_CONFLICT = 3;

    /**
     * Error code returned if the user is denied to access the timeline data
     */
    public static final int ACCESS_DENIED = 4;

    /**
     * Error code returned if the entity doesn't have an valid domain ID
     */
    public static final int NO_DOMAIN = 5;

    /**
     * Error code returned if the user is denied to relate the entity to another
     * one in different domain
     */
    public static final int FORBIDDEN_RELATION = 6;

    /**
     * Error code returned if the entity start time is before the eviction
     * period of old data.
     */
    public static final int EXPIRED_ENTITY = 7;

    private String entityId;
    private String entityType;
    private int errorCode;

    /**
     * Get the entity Id
     * 
     * @return the entity Id
     */
    @XmlElement(name = "entity")
    public String getEntityId() {
      return entityId;
    }

    /**
     * Set the entity Id
     * 
     * @param entityId
     *          the entity Id
     */
    public void setEntityId(String entityId) {
      this.entityId = entityId;
    }

    /**
     * Get the entity type
     * 
     * @return the entity type
     */
    @XmlElement(name = "entitytype")
    public String getEntityType() {
      return entityType;
    }

    /**
     * Set the entity type
     * 
     * @param entityType
     *          the entity type
     */
    public void setEntityType(String entityType) {
      this.entityType = entityType;
    }

    /**
     * Get the error code
     * 
     * @return an error code
     */
    @XmlElement(name = "errorcode")
    public int getErrorCode() {
      return errorCode;
    }

    /**
     * Set the error code to the given error code
     * 
     * @param errorCode
     *          an error code
     */
    public void setErrorCode(int errorCode) {
      this.errorCode = errorCode;
    }

  }

}
