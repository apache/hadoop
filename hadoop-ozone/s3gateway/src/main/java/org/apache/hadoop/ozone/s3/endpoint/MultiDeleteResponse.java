/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.endpoint;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * Response for multi object delete request.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "DeleteResult", namespace = "http://s3.amazonaws"
    + ".com/doc/2006-03-01/")
public class MultiDeleteResponse {

  @XmlElement(name = "Deleted")
  private List<DeletedObject> deletedObjects = new ArrayList<>();

  @XmlElement(name = "Error")
  private List<Error> errors = new ArrayList<>();

  public void addDeleted(DeletedObject deletedObject) {
    deletedObjects.add(deletedObject);
  }

  public void addError(Error error) {
    errors.add(error);
  }

  public List<DeletedObject> getDeletedObjects() {
    return deletedObjects;
  }

  public void setDeletedObjects(
      List<DeletedObject> deletedObjects) {
    this.deletedObjects = deletedObjects;
  }

  public List<Error> getErrors() {
    return errors;
  }

  public void setErrors(
      List<Error> errors) {
    this.errors = errors;
  }

  /**
   * JAXB entity for child element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Deleted", namespace = "http://s3.amazonaws"
      + ".com/doc/2006-03-01/")
  public static class DeletedObject {

    @XmlElement(name = "Key")
    private String key;

    private String versionId;

    public DeletedObject() {
    }

    public DeletedObject(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getVersionId() {
      return versionId;
    }

    public void setVersionId(String versionId) {
      this.versionId = versionId;
    }
  }

  /**
   * JAXB entity for child element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Error", namespace = "http://s3.amazonaws"
      + ".com/doc/2006-03-01/")
  public static class Error {

    @XmlElement(name = "Key")
    private String key;

    @XmlElement(name = "Code")
    private String code;

    @XmlElement(name = "Message")
    private String message;

    public Error() {
    }

    public Error(String key, String code, String message) {
      this.key = key;
      this.code = code;
      this.message = message;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getCode() {
      return code;
    }

    public void setCode(String code) {
      this.code = code;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }
}
