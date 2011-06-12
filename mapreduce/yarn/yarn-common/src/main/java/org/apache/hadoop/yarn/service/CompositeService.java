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

package org.apache.hadoop.yarn.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;

/**
 * Composition of services.
 */
public class CompositeService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(CompositeService.class);

  private List<Service> serviceList = new ArrayList<Service>();

  public CompositeService(String name) {
    super(name);
  }

  public Collection<Service> getServices() {
    return Collections.unmodifiableList(serviceList);
  }

  protected synchronized void addService(Service service) {
    serviceList.add(service);
  }

  protected synchronized boolean removeService(Service service) {
    return serviceList.remove(service);
  }

  public synchronized void init(Configuration conf) {
    for (Service service : serviceList) {
      service.init(conf);
    }
    super.init(conf);
  }

  public synchronized void start() {
    int i = 0;
    try {
      for (int n = serviceList.size(); i < n; i++) {
        Service service = serviceList.get(i);
        service.start();
      }
    } catch(Throwable e) {
      LOG.error("Error starting services " + getName(), e);
      for (int j = i-1; j >= 0; j--) {
        Service service = serviceList.get(j);
        try {
          service.stop();
        } catch(Throwable t) {
          LOG.info("Error stopping " + service.getName(), t);
        }
      }
      throw new YarnException("Failed to Start " + getName(), e);
    }
    super.start();
  }

  public synchronized void stop() {
    //stop in reserve order of start
    for (int i = serviceList.size() - 1; i >= 0; i--) {
      Service service = serviceList.get(i);
      service.stop();
    }
    super.stop();
  }

}
