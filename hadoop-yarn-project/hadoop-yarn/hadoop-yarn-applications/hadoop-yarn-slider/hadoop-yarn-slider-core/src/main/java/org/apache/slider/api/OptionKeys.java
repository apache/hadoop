/*
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

package org.apache.slider.api;

/**
 *  Keys for entries in the <code>options</code> section
 *  of a cluster description.
 */
public interface OptionKeys extends InternalKeys {

  /**
   * Time in milliseconds to wait after forking any in-AM 
   * process before attempting to start up the containers: {@value}
   * 
   * A shorter value brings the cluster up faster, but means that if the
   * in AM process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  String APPLICATION_TYPE = "application.type";
  
  String APPLICATION_NAME = "application.name";

  /**
   * Prefix for site.xml options: {@value}
   */
  String SITE_XML_PREFIX = "site.";
  /**
   * Prefix for config file options: {@value}
   */
  String CONF_FILE_PREFIX = "conf.";
  /**
   * Prefix for package options: {@value}
   */
  String PKG_FILE_PREFIX = "pkg.";
  /**
   * Prefix for export options: {@value}
   */
  String EXPORT_PREFIX = "export.";
  /**
   * Type suffix for config file and package options: {@value}
   */
  String TYPE_SUFFIX = ".type";
  /**
   * Name suffix for config file and package options: {@value}
   */
  String NAME_SUFFIX = ".name";
  /**
   * Per component suffix for config file options: {@value}
   */
  String PER_COMPONENT = ".per.component";
  /**
   * Per group suffix for config file options: {@value}
   */
  String PER_GROUP = ".per.group";

  /**
   * Zookeeper quorum host list: {@value}
   */
  String ZOOKEEPER_QUORUM = "zookeeper.quorum";
  String ZOOKEEPER_HOSTS = "zookeeper.hosts";
  String ZOOKEEPER_PORT = "zookeeper.port";

  /**
   * Zookeeper path value (string): {@value}
   */
  String ZOOKEEPER_PATH = "zookeeper.path";

}
