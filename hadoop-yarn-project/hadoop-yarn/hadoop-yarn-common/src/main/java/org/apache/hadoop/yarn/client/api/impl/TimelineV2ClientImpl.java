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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Implementation of timeline v2 client interface.
 *
 */
public class TimelineV2ClientImpl extends TimelineV2Client {
  private static final Log LOG = LogFactory.getLog(TimelineV2ClientImpl.class);

  private static final String RESOURCE_URI_STR_V2 = "/ws/v2/timeline/";

  private TimelineEntityDispatcher entityDispatcher;
  private volatile String timelineServiceAddress;
  @VisibleForTesting
  volatile Token currentTimelineToken = null;

  // Retry parameters for identifying new timeline service
  // TODO consider to merge with connection retry
  private int maxServiceRetries;
  private long serviceRetryInterval;

  private TimelineConnector connector;

  private ApplicationId contextAppId;

  private UserGroupInformation authUgi;

  public TimelineV2ClientImpl(ApplicationId appId) {
    super(TimelineV2ClientImpl.class.getName());
    this.contextAppId = appId;
  }

  public ApplicationId getContextAppId() {
    return contextAppId;
  }

  protected void serviceInit(Configuration conf) throws Exception {
    if (!YarnConfiguration.timelineServiceEnabled(conf)
        || (int) YarnConfiguration.getTimelineServiceVersion(conf) != 2) {
      throw new IOException("Timeline V2 client is not properly configured. "
          + "Either timeline service is not enabled or version is not set to"
          + " 2");
    }
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUgi = ugi.getRealUser();
    String doAsUser = null;
    if (realUgi != null) {
      authUgi = realUgi;
      doAsUser = ugi.getShortUserName();
    } else {
      authUgi = ugi;
      doAsUser = null;
    }
    // TODO need to add/cleanup filter retry later for ATSV2. similar to V1
    DelegationTokenAuthenticatedURL.Token token =
        new DelegationTokenAuthenticatedURL.Token();
    connector = new TimelineConnector(false, authUgi, doAsUser, token);
    addIfService(connector);

    // new version need to auto discovery (with retry till ATS v2 address is
    // got).
    maxServiceRetries =
        conf.getInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
    serviceRetryInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
    entityDispatcher = new TimelineEntityDispatcher(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    entityDispatcher.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    entityDispatcher.stop();
    super.serviceStop();
  }

  @Override
  public void putEntities(TimelineEntity... entities)
      throws IOException, YarnException {
    entityDispatcher.dispatchEntities(true, entities);
  }

  @Override
  public void putEntitiesAsync(TimelineEntity... entities)
      throws IOException, YarnException {
    entityDispatcher.dispatchEntities(false, entities);
  }

  @Override
  public void setTimelineCollectorInfo(CollectorInfo collectorInfo) {
    if (collectorInfo == null) {
      LOG.warn("Not setting collector info as it is null.");
      return;
    }
    // First update the token so that it is available when collector address is
    // used.
    if (collectorInfo.getCollectorToken() != null) {
      // Use collector address to update token service if its not available.
      setTimelineDelegationToken(
          collectorInfo.getCollectorToken(), collectorInfo.getCollectorAddr());
    }
    // Update timeline service address.
    if (collectorInfo.getCollectorAddr() != null &&
        !collectorInfo.getCollectorAddr().isEmpty() &&
        !collectorInfo.getCollectorAddr().equals(timelineServiceAddress)) {
      this.timelineServiceAddress = collectorInfo.getCollectorAddr();
      LOG.info("Updated timeline service address to " + timelineServiceAddress);
    }
  }

  private void setTimelineDelegationToken(Token delegationToken,
      String collectorAddr) {
    // Checks below are to ensure that an invalid token is not updated in UGI.
    // This is required because timeline token is set via a public API.
    if (!delegationToken.getKind().equals(
        TimelineDelegationTokenIdentifier.KIND_NAME.toString())) {
      LOG.warn("Timeline token to be updated should be of kind " +
          TimelineDelegationTokenIdentifier.KIND_NAME);
      return;
    }
    if (collectorAddr == null || collectorAddr.isEmpty()) {
      collectorAddr = timelineServiceAddress;
    }
    // Token need not be updated if both address and token service do not exist.
    String service = delegationToken.getService();
    if ((service == null || service.isEmpty()) &&
        (collectorAddr == null || collectorAddr.isEmpty())) {
      LOG.warn("Timeline token does not have service and timeline service " +
          "address is not yet set. Not updating the token");
      return;
    }
    // No need to update a duplicate token.
    if (currentTimelineToken != null &&
        currentTimelineToken.equals(delegationToken)) {
      return;
    }
    currentTimelineToken = delegationToken;
    // Convert the token, sanitize the token service and add it to UGI.
    org.apache.hadoop.security.token.
        Token<TimelineDelegationTokenIdentifier> timelineToken =
            new org.apache.hadoop.security.token.
            Token<TimelineDelegationTokenIdentifier>(
                delegationToken.getIdentifier().array(),
                delegationToken.getPassword().array(),
                new Text(delegationToken.getKind()),
                service == null ? new Text() : new Text(service));
    // Prefer timeline service address over service coming in the token for
    // updating the token service.
    InetSocketAddress serviceAddr =
        (collectorAddr != null && !collectorAddr.isEmpty()) ?
        NetUtils.createSocketAddr(collectorAddr) :
        SecurityUtil.getTokenServiceAddr(timelineToken);
    SecurityUtil.setTokenService(timelineToken, serviceAddr);
    authUgi.addToken(timelineToken);
    LOG.info("Updated timeline delegation token " + timelineToken);
  }

  @Private
  protected void putObjects(String path, MultivaluedMap<String, String> params,
      Object obj) throws IOException, YarnException {

    int retries = verifyRestEndPointAvailable();

    // timelineServiceAddress could be stale, add retry logic here.
    boolean needRetry = true;
    while (needRetry) {
      try {
        URI uri = TimelineConnector.constructResURI(getConfig(),
            timelineServiceAddress, RESOURCE_URI_STR_V2);
        putObjects(uri, path, params, obj);
        needRetry = false;
      } catch (IOException e) {
        // handle exception for timelineServiceAddress being updated.
        checkRetryWithSleep(retries, e);
        retries--;
      }
    }
  }

  /**
   * Check if reaching to maximum of retries.
   *
   * @param retries
   * @param e
   */
  private void checkRetryWithSleep(int retries, IOException e)
      throws YarnException, IOException {
    if (retries > 0) {
      try {
        Thread.sleep(this.serviceRetryInterval);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new YarnException("Interrupted while retrying to connect to ATS");
      }
    } else {
      StringBuilder msg =
          new StringBuilder("TimelineClient has reached to max retry times : ");
      msg.append(this.maxServiceRetries);
      msg.append(" for service address: ");
      msg.append(timelineServiceAddress);
      LOG.error(msg.toString());
      throw new IOException(msg.toString(), e);
    }
  }

  private ClientResponse doPutObjects(URI base, String path,
      MultivaluedMap<String, String> params, Object obj) {
    return connector.getClient().resource(base).path(path).queryParams(params)
        .accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
        .put(ClientResponse.class, obj);
  }

  protected void putObjects(URI base, String path,
      MultivaluedMap<String, String> params, Object obj)
      throws IOException, YarnException {
    ClientResponse resp = null;
    try {
      resp = authUgi.doAs(new PrivilegedExceptionAction<ClientResponse>() {
        @Override
        public ClientResponse run() throws Exception {
          return doPutObjects(base, path, params, obj);
        }
      });
    } catch (UndeclaredThrowableException ue) {
      Throwable cause = ue.getCause();
      if (cause instanceof IOException) {
        throw (IOException)cause;
      } else {
        throw new IOException(cause);
      }
    } catch (InterruptedException ie) {
      throw (IOException) new InterruptedIOException().initCause(ie);
    }
    if (resp == null || resp.getStatusInfo()
        .getStatusCode() != ClientResponse.Status.OK.getStatusCode()) {
      String msg =
          "Response from the timeline server is " + ((resp == null) ? "null"
              : "not successful," + " HTTP error code: " + resp.getStatus()
                  + ", Server response:\n" + resp.getEntity(String.class));
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  private int verifyRestEndPointAvailable() throws YarnException {
    // timelineServiceAddress could haven't be initialized yet
    // or stale (only for new timeline service)
    int retries = pollTimelineServiceAddress(this.maxServiceRetries);
    if (timelineServiceAddress == null) {
      String errMessage = "TimelineClient has reached to max retry times : "
          + this.maxServiceRetries
          + ", but failed to fetch timeline service address. Please verify"
          + " Timeline Auxiliary Service is configured in all the NMs";
      LOG.error(errMessage);
      throw new YarnException(errMessage);
    }
    return retries;
  }

  /**
   * Poll TimelineServiceAddress for maximum of retries times if it is null.
   *
   * @param retries
   * @return the left retry times
   * @throws IOException
   */
  private int pollTimelineServiceAddress(int retries) throws YarnException {
    while (timelineServiceAddress == null && retries > 0) {
      try {
        Thread.sleep(this.serviceRetryInterval);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new YarnException("Interrupted while trying to connect ATS");
      }
      retries--;
    }
    return retries;
  }

  private final class EntitiesHolder extends FutureTask<Void> {
    private final TimelineEntities entities;
    private final boolean isSync;

    EntitiesHolder(final TimelineEntities entities, final boolean isSync) {
      super(new Callable<Void>() {
        // publishEntities()
        public Void call() throws Exception {
          MultivaluedMap<String, String> params = new MultivaluedMapImpl();
          params.add("appid", getContextAppId().toString());
          params.add("async", Boolean.toString(!isSync));
          putObjects("entities", params, entities);
          return null;
        }
      });
      this.entities = entities;
      this.isSync = isSync;
    }

    public boolean isSync() {
      return isSync;
    }

    public TimelineEntities getEntities() {
      return entities;
    }
  }

  /**
   * This class is responsible for collecting the timeline entities and
   * publishing them in async.
   */
  private class TimelineEntityDispatcher {
    /**
     * Time period for which the timelineclient will wait for draining after
     * stop.
     */
    private final long drainTimeoutPeriod;

    private int numberOfAsyncsToMerge;
    private final BlockingQueue<EntitiesHolder> timelineEntityQueue;
    private ExecutorService executor;

    TimelineEntityDispatcher(Configuration conf) {
      timelineEntityQueue = new LinkedBlockingQueue<EntitiesHolder>();
      numberOfAsyncsToMerge =
          conf.getInt(YarnConfiguration.NUMBER_OF_ASYNC_ENTITIES_TO_MERGE,
              YarnConfiguration.DEFAULT_NUMBER_OF_ASYNC_ENTITIES_TO_MERGE);
      drainTimeoutPeriod = conf.getLong(
          YarnConfiguration.TIMELINE_V2_CLIENT_DRAIN_TIME_MILLIS,
          YarnConfiguration.DEFAULT_TIMELINE_V2_CLIENT_DRAIN_TIME_MILLIS);
    }

    Runnable createRunnable() {
      return new Runnable() {
        @Override
        public void run() {
          try {
            EntitiesHolder entitiesHolder;
            while (!Thread.currentThread().isInterrupted()) {
              // Merge all the async calls and make one push, but if its sync
              // call push immediately
              try {
                entitiesHolder = timelineEntityQueue.take();
              } catch (InterruptedException ie) {
                LOG.info("Timeline dispatcher thread was interrupted ");
                Thread.currentThread().interrupt();
                return;
              }
              if (entitiesHolder != null) {
                publishWithoutBlockingOnQueue(entitiesHolder);
              }
            }
          } finally {
            if (!timelineEntityQueue.isEmpty()) {
              LOG.info("Yet to publish " + timelineEntityQueue.size()
                  + " timelineEntities, draining them now. ");
            }
            // Try to drain the remaining entities to be published @ the max for
            // 2 seconds
            long timeTillweDrain =
                System.currentTimeMillis() + drainTimeoutPeriod;
            while (!timelineEntityQueue.isEmpty()) {
              publishWithoutBlockingOnQueue(timelineEntityQueue.poll());
              if (System.currentTimeMillis() > timeTillweDrain) {
                // time elapsed stop publishing further....
                if (!timelineEntityQueue.isEmpty()) {
                  LOG.warn("Time to drain elapsed! Remaining "
                      + timelineEntityQueue.size() + "timelineEntities will not"
                      + " be published");
                  // if some entities were not drained then we need interrupt
                  // the threads which had put sync EntityHolders to the queue.
                  EntitiesHolder nextEntityInTheQueue = null;
                  while ((nextEntityInTheQueue =
                      timelineEntityQueue.poll()) != null) {
                    nextEntityInTheQueue.cancel(true);
                  }
                }
                break;
              }
            }
          }
        }

        /**
         * Publishes the given EntitiesHolder and return immediately if sync
         * call, else tries to fetch the EntitiesHolder from the queue in non
         * blocking fashion and collate the Entities if possible before
         * publishing through REST.
         *
         * @param entitiesHolder
         */
        private void publishWithoutBlockingOnQueue(
            EntitiesHolder entitiesHolder) {
          if (entitiesHolder.isSync()) {
            entitiesHolder.run();
            return;
          }
          int count = 1;
          while (true) {
            // loop till we find a sync put Entities or there is nothing
            // to take
            EntitiesHolder nextEntityInTheQueue = timelineEntityQueue.poll();
            if (nextEntityInTheQueue == null) {
              // Nothing in the queue just publish and get back to the
              // blocked wait state
              entitiesHolder.run();
              break;
            } else if (nextEntityInTheQueue.isSync()) {
              // flush all the prev async entities first
              entitiesHolder.run();
              // and then flush the sync entity
              nextEntityInTheQueue.run();
              break;
            } else {
              // append all async entities together and then flush
              entitiesHolder.getEntities().addEntities(
                  nextEntityInTheQueue.getEntities().getEntities());
              count++;
              if (count == numberOfAsyncsToMerge) {
                // Flush the entities if the number of the async
                // putEntites merged reaches the desired limit. To avoid
                // collecting multiple entities and delaying for a long
                // time.
                entitiesHolder.run();
                break;
              }
            }
          }
        }
      };
    }

    public void dispatchEntities(boolean sync,
        TimelineEntity[] entitiesTobePublished) throws YarnException {
      if (executor.isShutdown()) {
        throw new YarnException("Timeline client is in the process of stopping,"
            + " not accepting any more TimelineEntities");
      }

      // wrap all TimelineEntity into TimelineEntities object
      TimelineEntities entities = new TimelineEntities();
      for (TimelineEntity entity : entitiesTobePublished) {
        entities.addEntity(entity);
      }

      // created a holder and place it in queue
      EntitiesHolder entitiesHolder = new EntitiesHolder(entities, sync);
      try {
        timelineEntityQueue.put(entitiesHolder);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new YarnException(
            "Failed while adding entity to the queue for publishing", e);
      }

      if (sync) {
        // In sync call we need to wait till its published and if any error then
        // throw it back
        try {
          entitiesHolder.get();
        } catch (ExecutionException e) {
          throw new YarnException("Failed while publishing entity",
              e.getCause());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new YarnException("Interrupted while publishing entity", e);
        }
      }
    }

    public void start() {
      executor = Executors.newSingleThreadExecutor();
      executor.execute(createRunnable());
    }

    public void stop() {
      LOG.info("Stopping TimelineClient.");
      executor.shutdownNow();
      try {
        executor.awaitTermination(drainTimeoutPeriod, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
      }
    }
  }
}
