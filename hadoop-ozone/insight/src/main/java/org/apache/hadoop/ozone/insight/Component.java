/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import java.util.Objects;

/**
 * Identifier an ozone component.
 */
public class Component {

  /**
   * The type of the component (eg. scm, s3g...)
   */
  private Type name;

  /**
   * Unique identifier of the instance (uuid or index). Can be null for
   * non-HA server component.
   */
  private String id;

  /**
   * Hostname of the component. Optional, may help to find the right host
   * name.
   */
  private String hostname;

  /**
   * HTTP service port. Optional.
   */
  private int port;

  public Component(Type name) {
    this.name = name;
  }

  public Component(Type name, String id) {
    this.name = name;
    this.id = id;
  }

  public Component(Type name, String id, String hostname) {
    this.name = name;
    this.id = id;
    this.hostname = hostname;
  }

  public Component(Type name, String id, String hostname, int port) {
    this.name = name;
    this.id = id;
    this.hostname = hostname;
    this.port = port;
  }

  public Type getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Component that = (Component) o;
    return Objects.equals(name, that.name) &&
        Objects.equals(id, that.id);
  }

  public String prefix() {
    return name + (id != null && id.length() > 0 ? "-" + id : "");
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, id);
  }

  /**
   * Ozone component types.
   */
  public enum Type {
    SCM, OM, DATANODE, S3G, RECON;
  }

}
