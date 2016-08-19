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
package org.apache.hadoop.yarn.api.records.timelineservice;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * A class that holds a list of put errors. This is the response returned when a
 * list of {@link TimelineEntity} objects is added to the timeline. If there are
 * errors in storing individual entity objects, they will be indicated in the
 * list of errors.
 */
@XmlRootElement(name = "response")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Unstable
public class TimelineWriteResponse {

  private List<TimelineWriteError> errors = new ArrayList<TimelineWriteError>();

  public TimelineWriteResponse() {

  }

  /**
   * Get a list of {@link TimelineWriteError} instances.
   *
   * @return a list of {@link TimelineWriteError} instances
   */
  @XmlElement(name = "errors")
  public List<TimelineWriteError> getErrors() {
    return errors;
  }

  /**
   * Add a single {@link TimelineWriteError} instance into the existing list.
   *
   * @param error
   *          a single {@link TimelineWriteError} instance
   */
  public void addError(TimelineWriteError error) {
    errors.add(error);
  }

  /**
   * Add a list of {@link TimelineWriteError} instances into the existing list.
   *
   * @param writeErrors
   *          a list of {@link TimelineWriteError} instances
   */
  public void addErrors(List<TimelineWriteError> writeErrors) {
    this.errors.addAll(writeErrors);
  }

  /**
   * Set the list to the given list of {@link TimelineWriteError} instances.
   *
   * @param writeErrors
   *          a list of {@link TimelineWriteError} instances
   */
  public void setErrors(List<TimelineWriteError> writeErrors) {
    this.errors.clear();
    this.errors.addAll(writeErrors);
  }

  /**
   * A class that holds the error code for one entity.
   */
  @XmlRootElement(name = "error")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Unstable
  public static class TimelineWriteError {

    /**
     * Error code returned if an IOException is encountered when storing an
     * entity.
     */
    public static final int IO_EXCEPTION = 1;

    private String entityId;
    private String entityType;
    private int errorCode;

    /**
     * Get the entity Id.
     *
     * @return the entity Id
     */
    @XmlElement(name = "entity")
    public String getEntityId() {
      return entityId;
    }

    /**
     * Set the entity Id.
     *
     * @param id the entity Id.
     */
    public void setEntityId(String id) {
      this.entityId = id;
    }

    /**
     * Get the entity type.
     *
     * @return the entity type
     */
    @XmlElement(name = "entitytype")
    public String getEntityType() {
      return entityType;
    }

    /**
     * Set the entity type.
     *
     * @param type the entity type.
     */
    public void setEntityType(String type) {
      this.entityType = type;
    }

    /**
     * Get the error code.
     *
     * @return an error code
     */
    @XmlElement(name = "errorcode")
    public int getErrorCode() {
      return errorCode;
    }

    /**
     * Set the error code to the given error code.
     *
     * @param code an error code.
     */
    public void setErrorCode(int code) {
      this.errorCode = code;
    }

  }

}
