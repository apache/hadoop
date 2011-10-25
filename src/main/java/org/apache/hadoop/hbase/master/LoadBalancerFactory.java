/**
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The class that creates a load balancer from a conf.
 */
public class LoadBalancerFactory {

  /**
   * Create a loadblanacer from the given conf.
   * @param conf
   * @return A {@link LoadBalancer}
   */
  public static LoadBalancer getLoadBalancer(Configuration conf) {

    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass = conf.getClass("hbase.maser.loadBalancer.class",DefaultLoadBalancer.class, LoadBalancer.class);
    return ReflectionUtils.newInstance(balancerKlass, conf);

  }
}
