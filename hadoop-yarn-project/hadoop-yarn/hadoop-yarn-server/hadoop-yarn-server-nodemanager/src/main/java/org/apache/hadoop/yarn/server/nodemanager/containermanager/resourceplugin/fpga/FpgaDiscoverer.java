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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class FpgaDiscoverer {

  public static final Logger LOG = LoggerFactory.getLogger(
      FpgaDiscoverer.class);

  private static FpgaDiscoverer instance;

  private Configuration conf = null;

  private AbstractFpgaVendorPlugin plugin = null;

  private List<FpgaResourceAllocator.FpgaDevice> currentFpgaInfo = null;

  // shell command timeout
  private static final int MAX_EXEC_TIMEOUT_MS = 10 * 1000;

  static {
    instance = new FpgaDiscoverer();
  }

  public static FpgaDiscoverer getInstance() {
    return instance;
  }

  @VisibleForTesting
  public synchronized static FpgaDiscoverer setInstance(FpgaDiscoverer newInstance) {
    instance = newInstance;
    return instance;
  }

  @VisibleForTesting
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }

  public List<FpgaResourceAllocator.FpgaDevice> getCurrentFpgaInfo() {
    return currentFpgaInfo;
  }

  public synchronized void setResourceHanderPlugin(AbstractFpgaVendorPlugin plugin) {
    this.plugin = plugin;
  }

  public synchronized boolean diagnose() {
    return this.plugin.diagnose(MAX_EXEC_TIMEOUT_MS);
  }

  public synchronized void initialize(Configuration conf) throws YarnException {
    this.conf = conf;
    this.plugin.initPlugin(conf);
    // Try to diagnose FPGA
    LOG.info("Trying to diagnose FPGA information ...");
    if (!diagnose()) {
      LOG.warn("Failed to pass FPGA devices diagnose");
    }
  }

  /**
   * get avialable devices minor numbers from toolchain or static configuration
   * */
  public synchronized List<FpgaResourceAllocator.FpgaDevice> discover() throws ResourceHandlerException {
    List<FpgaResourceAllocator.FpgaDevice> list;
    String allowed = this.conf.get(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES);
    // whatever static or auto discover, we always needs
    // the vendor plugin to discover. For instance, IntelFpgaOpenclPlugin need to
    // setup a mapping of <major:minor> to <aliasDevName>
    list = this.plugin.discover(MAX_EXEC_TIMEOUT_MS);
    if (0 == list.size()) {
      throw new ResourceHandlerException("No FPGA devices detected!");
    }
    currentFpgaInfo = list;
    if (allowed.equalsIgnoreCase(
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES)) {
        return list;
    } else if (allowed.matches("(\\d,)*\\d")){
      String[] minors = allowed.split(",");
      Iterator<FpgaResourceAllocator.FpgaDevice> iterator = list.iterator();
      // remove the non-configured minor numbers
      FpgaResourceAllocator.FpgaDevice t;
      while (iterator.hasNext()) {
        boolean valid = false;
        t = iterator.next();
        for (String minorNumber : minors) {
          if (t.getMinor().toString().equals(minorNumber)) {
            valid = true;
            break;
          }
        }
        if (!valid) {
          iterator.remove();
        }
      }
      // if the count of user configured is still larger than actual
      if (list.size() != minors.length) {
        LOG.warn("We continue although there're mistakes in user's configuration " +
            YarnConfiguration.NM_FPGA_ALLOWED_DEVICES +
            "user configured:" + allowed + ", while the real:" + list.toString());
      }
    } else {
      throw new ResourceHandlerException("Invalid value configured for " +
          YarnConfiguration.NM_FPGA_ALLOWED_DEVICES + ":\"" + allowed + "\"");
    }
    return list;
  }

}
