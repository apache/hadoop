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

package org.apache.hadoop.yarn.server.timelineservice.aggregator;

import java.io.IOException;
import java.net.URI;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.AggregatorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoRequest;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;

/**
 * Class that manages adding and removing aggregators and their lifecycle. It
 * provides thread safety access to the aggregators inside.
 *
 * It is a singleton, and instances should be obtained via
 * {@link #getInstance()}.
 */
@Private
@Unstable
public class TimelineAggregatorsCollection extends CompositeService {
  private static final Log LOG =
      LogFactory.getLog(TimelineAggregatorsCollection.class);
  private static final TimelineAggregatorsCollection INSTANCE =
      new TimelineAggregatorsCollection();

  // access to this map is synchronized with the map itself
  private final Map<String, TimelineAggregator> aggregators =
      Collections.synchronizedMap(
          new HashMap<String, TimelineAggregator>());

  // REST server for this aggregator collection
  private HttpServer2 timelineRestServer;
  
  private String timelineRestServerBindAddress;
  
  private AggregatorNodemanagerProtocol nmAggregatorService;
  
  private InetSocketAddress nmAggregatorServiceAddress;

  static final String AGGREGATOR_COLLECTION_ATTR_KEY = "aggregator.collection";

  static TimelineAggregatorsCollection getInstance() {
    return INSTANCE;
  }

  TimelineAggregatorsCollection() {
    super(TimelineAggregatorsCollection.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.nmAggregatorServiceAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_AGGREGATOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_AGGREGATOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_AGGREGATOR_SERVICE_PORT);
    
  }
  
  @Override
  protected void serviceStart() throws Exception {
    startWebApp();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (timelineRestServer != null) {
      timelineRestServer.stop();
    }
    super.serviceStop();
  }

  /**
   * Put the aggregator into the collection if an aggregator mapped by id does
   * not exist.
   *
   * @throws YarnRuntimeException if there was any exception in initializing and
   * starting the app level service
   * @return the aggregator associated with id after the potential put.
   */
  public TimelineAggregator putIfAbsent(ApplicationId appId, 
      TimelineAggregator aggregator) {
    String id = appId.toString();
    TimelineAggregator aggregatorInTable;
    boolean aggregatorIsNew = false;
    synchronized (aggregators) {
      aggregatorInTable = aggregators.get(id);
      if (aggregatorInTable == null) {
        try {
          // initialize, start, and add it to the collection so it can be
          // cleaned up when the parent shuts down
          aggregator.init(getConfig());
          aggregator.start();
          aggregators.put(id, aggregator);
          LOG.info("the aggregator for " + id + " was added");
          aggregatorInTable = aggregator;
          aggregatorIsNew = true;
        } catch (Exception e) {
          throw new YarnRuntimeException(e);
        }
      } else {
        String msg = "the aggregator for " + id + " already exists!";
        LOG.error(msg);
      }
      
    }
    // Report to NM if a new aggregator is added.
    if (aggregatorIsNew) {
      try {
        reportNewAggregatorToNM(appId);
      } catch (Exception e) {
        // throw exception here as it cannot be used if failed report to NM
        LOG.error("Failed to report a new aggregator for application: " + appId + 
            " to NM Aggregator Services.");
        throw new YarnRuntimeException(e);
      }
    }
    
    return aggregatorInTable;
  }

  /**
   * Removes the aggregator for the specified id. The aggregator is also stopped
   * as a result. If the aggregator does not exist, no change is made.
   *
   * @return whether it was removed successfully
   */
  public boolean remove(String id) {
    synchronized (aggregators) {
      TimelineAggregator aggregator = aggregators.remove(id);
      if (aggregator == null) {
        String msg = "the aggregator for " + id + " does not exist!";
        LOG.error(msg);
        return false;
      } else {
        // stop the service to do clean up
        aggregator.stop();
        LOG.info("the aggregator service for " + id + " was removed");
        return true;
      }
    }
  }

  /**
   * Returns the aggregator for the specified id.
   *
   * @return the aggregator or null if it does not exist
   */
  public TimelineAggregator get(String id) {
    return aggregators.get(id);
  }

  /**
   * Returns whether the aggregator for the specified id exists in this
   * collection.
   */
  public boolean containsKey(String id) {
    return aggregators.containsKey(id);
  }

  /**
   * Launch the REST web server for this aggregator collection
   */
  private void startWebApp() {
    Configuration conf = getConfig();
    // use the same ports as the old ATS for now; we could create new properties
    // for the new timeline service if needed
    String bindAddress = WebAppUtils.getWebAppBindURL(conf,
        YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf));
    this.timelineRestServerBindAddress = WebAppUtils.getResolvedAddress(
        NetUtils.createSocketAddr(bindAddress));
    LOG.info("Instantiating the per-node aggregator webapp at " + 
        timelineRestServerBindAddress);
    try {
      Configuration confForInfoServer = new Configuration(conf);
      confForInfoServer.setInt(HttpServer2.HTTP_MAX_THREADS, 10);
      HttpServer2.Builder builder = new HttpServer2.Builder()
          .setName("timeline")
          .setConf(conf)
          .addEndpoint(URI.create("http://" + bindAddress));
      timelineRestServer = builder.build();
      // TODO: replace this by an authentication filter in future.
      HashMap<String, String> options = new HashMap<>();
      String username = conf.get(HADOOP_HTTP_STATIC_USER,
          DEFAULT_HADOOP_HTTP_STATIC_USER);
      options.put(HADOOP_HTTP_STATIC_USER, username);
      HttpServer2.defineFilter(timelineRestServer.getWebAppContext(),
          "static_user_filter_timeline",
          StaticUserWebFilter.StaticUserFilter.class.getName(),
          options, new String[] {"/*"});

      timelineRestServer.addJerseyResourcePackage(
          TimelineAggregatorWebService.class.getPackage().getName() + ";"
              + GenericExceptionHandler.class.getPackage().getName() + ";"
              + YarnJacksonJaxbJsonProvider.class.getPackage().getName(),
          "/*");
      timelineRestServer.setAttribute(AGGREGATOR_COLLECTION_ATTR_KEY,
          TimelineAggregatorsCollection.getInstance());
      timelineRestServer.start();
    } catch (Exception e) {
      String msg = "The per-node aggregator webapp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }
  
  private void reportNewAggregatorToNM(ApplicationId appId) 
      throws YarnException, IOException {
    this.nmAggregatorService = getNMAggregatorService();
    ReportNewAggregatorsInfoRequest request = 
        ReportNewAggregatorsInfoRequest.newInstance(appId,
            this.timelineRestServerBindAddress);
    LOG.info("Report a new aggregator for application: " + appId + 
        " to NM Aggregator Services.");
    nmAggregatorService.reportNewAggregatorInfo(request);
  }
  
  // protected for test
  protected AggregatorNodemanagerProtocol getNMAggregatorService(){
    Configuration conf = getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    
    // TODO Security settings.
    return (AggregatorNodemanagerProtocol) rpc.getProxy(
        AggregatorNodemanagerProtocol.class,
        nmAggregatorServiceAddress, conf);
  }
  
}
