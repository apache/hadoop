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

/**
 * Core Zookeeper support.
 * <p>
 * This package contains the low-level bindings to Curator and Zookeeper,
 * including everything related to registry security.
 * <p>
 * The class {@link org.apache.hadoop.registry.client.impl.zk.CuratorService}
 * is a YARN service which offers access to a Zookeeper instance via
 * Apache Curator.
 * <p>
 * The {@link org.apache.hadoop.registry.client.impl.zk.RegistrySecurity}
 * implements the security support in the registry, though a set of
 * static methods and as a YARN service.
 * <p>
 * To work with ZK, system properties need to be set before invoking
 * some operations/instantiating some objects. The definitions of these
 * are kept in {@link org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions}.
 *
 *
 */
package org.apache.hadoop.registry.client.impl.zk;
