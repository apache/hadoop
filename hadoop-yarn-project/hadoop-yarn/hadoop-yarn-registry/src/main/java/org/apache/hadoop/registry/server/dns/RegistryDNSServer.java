/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.registry.server.dns;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.DNSOperationsFactory;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.impl.zk.PathListener;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A server/service that starts and manages the lifecycle of a DNS registry
 * instance.
 */
public class RegistryDNSServer extends CompositeService {


  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private RegistryDNS registryDNS;
  private RegistryOperationsService registryOperations;
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryDNS.class);
  private ConcurrentMap<String, ServiceRecord> pathToRecordMap;

  /**
   * Creates the DNS server.
   * @param name the server name.
   * @param registryDNS the registry DNS instance.
   */
  public RegistryDNSServer(String name, final RegistryDNS registryDNS) {
    super(name);
    this.registryDNS = registryDNS;
  }

  /**
   * Initializes the DNS server.
   * @param conf the hadoop configuration instance.
   * @throws Exception if service initialization fails.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    pathToRecordMap = new ConcurrentHashMap<>();

    registryOperations = new RegistryOperationsService("RegistryDNSOperations");
    addService(registryOperations);

    if (registryDNS == null) {
      registryDNS = (RegistryDNS) DNSOperationsFactory.createInstance(conf);
    }
    addService(registryDNS);

    super.serviceInit(conf);
  }

  /**
   * Starts the server.
   * @throws Exception if service start fails.
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    manageRegistryDNS();
  }

  /**
   * Performs operations required to setup the DNS registry instance (e.g. sets
   * up a path listener to react to service record creation/deletion and invoke
   * the appropriate registry method).
   */
  private void manageRegistryDNS() {

    try {
      registryOperations.monitorRegistryEntries();
      registryOperations.registerPathListener(new PathListener() {
        private String registryRoot = getConfig().
            get(RegistryConstants.KEY_REGISTRY_ZK_ROOT,
                RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);

        @Override
        public void nodeAdded(String path) throws IOException {
          // get a listing of service records
          String relativePath = getPathRelativeToRegistryRoot(path);
          String child = RegistryPathUtils.lastPathEntry(path);
          Map<String, RegistryPathStatus> map = new HashMap<>();
          map.put(child, registryOperations.stat(relativePath));
          Map<String, ServiceRecord> records =
              RegistryUtils.extractServiceRecords(registryOperations,
                                                  getAdjustedParentPath(path),
                                                  map);
          processServiceRecords(records, register);
          pathToRecordMap.putAll(records);
        }

        private String getAdjustedParentPath(String path) {
          Preconditions.checkNotNull(path);
          String adjustedPath = null;
          adjustedPath = getPathRelativeToRegistryRoot(path);
          try {
            return RegistryPathUtils.parentOf(adjustedPath);
          } catch (PathNotFoundException e) {
            // attempt to use passed in path
            return path;
          }
        }

        private String getPathRelativeToRegistryRoot(String path) {
          String adjustedPath;
          if (path.equals(registryRoot)) {
            adjustedPath = "/";
          } else {
            adjustedPath = path.substring(registryRoot.length());
          }
          return adjustedPath;
        }

        @Override
        public void nodeRemoved(String path) throws IOException {
          ServiceRecord record = pathToRecordMap.remove(path.substring(
              registryRoot.length()));
          processServiceRecord(path, record, delete);
        }

      });

      // create listener for record deletions

    } catch (Exception e) {
      LOG.warn("Unable to monitor the registry.  DNS support disabled.", e);
    }
  }

  /**
   * A registry management command interface.
   */
  interface ManagementCommand {
    void exec(String path, ServiceRecord record) throws IOException;
  }

  /**
   * Performs registry service record registration.
   */
  private final ManagementCommand register = new ManagementCommand() {
    @Override
    public void exec(String path, ServiceRecord record) throws IOException {
      if (record != null) {
        LOG.info("Registering DNS records for {}", path);
        registryDNS.register(path, record);
      }
    }
  };

  /**
   * Performs registry service record deletion.
   */
  private ManagementCommand delete = new ManagementCommand() {
    @Override
    public void exec(String path, ServiceRecord record) throws IOException {
      if (record != null) {
        LOG.info("Deleting DNS records for {}", path);
        registryDNS.delete(path, record);
      }
    }
  };

  /**
   * iterates thru the supplied service records, executing the provided registry
   * command.
   * @param records the service records.
   * @param command the registry command.
   * @throws IOException
   */
  private void processServiceRecords(Map<String, ServiceRecord> records,
                                     ManagementCommand command)
      throws IOException {
    for (Map.Entry<String, ServiceRecord> entry : records.entrySet()) {
      processServiceRecord(entry.getKey(), entry.getValue(), command);
    }
  }

  /**
   * Process the service record, parsing the information and creating the
   * required DNS records.
   * @param path  the service record path.
   * @param record  the record.
   * @param command  the registry command to execute.
   * @throws IOException
   */
  private void processServiceRecord(String path, ServiceRecord record,
                                     ManagementCommand command)
      throws IOException {
    command.exec(path, record);
  }

  /**
   * Launch the server.
   * @param conf configuration
   * @param rdns registry dns instance
   * @return
   */
  static RegistryDNSServer launchDNSServer(Configuration conf,
      RegistryDNS rdns) {
    RegistryDNSServer dnsServer = null;

    Thread
        .setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    try {
      dnsServer = new RegistryDNSServer("RegistryDNSServer", rdns);
      ShutdownHookManager.get().addShutdownHook(
          new CompositeService.CompositeServiceShutdownHook(dnsServer),
          SHUTDOWN_HOOK_PRIORITY);
      dnsServer.init(conf);
      dnsServer.start();
    } catch (Throwable t) {
      LOG.error("Error starting Registry DNS Server", t);
      ExitUtil.terminate(-1, "Error starting Registry DNS Server");
    }
    return dnsServer;
  }

  /**
   * Lanches the server instance.
   * @param args the command line args.
   * @throws IOException if command line options can't be parsed
   */
  public static void main(String[] args) throws IOException {
    StringUtils.startupShutdownMessage(RegistryDNSServer.class, args, LOG);
    YarnConfiguration conf = new YarnConfiguration();
    new GenericOptionsParser(conf, args);
    launchDNSServer(conf, null);
  }
}
