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

package org.apache.hadoop.yarn.webapp;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Contains the exception information from an exception thrown
 * by the web service REST API's.
 * Fields include:
 *   exception - exception type
 *   javaClassName - java class name of the exception
 *   message - a detailed message explaining the exception
 *
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
@XmlRootElement(name = "RemoteException")
@XmlAccessorType(XmlAccessType.FIELD)
public class RemoteExceptionData {

  private String exception;
  private String message;
  private String javaClassName;

  public RemoteExceptionData() {
  }

  public RemoteExceptionData(String excep, String message, String className) {
    this.exception = excep;
    this.message = message;
    this.javaClassName = className;
  }

  public String getException() {
    return exception;
  }

  public String getMessage() {
    return message;
  }

  public String getJavaClassName() {
    return javaClassName;
  }

}
