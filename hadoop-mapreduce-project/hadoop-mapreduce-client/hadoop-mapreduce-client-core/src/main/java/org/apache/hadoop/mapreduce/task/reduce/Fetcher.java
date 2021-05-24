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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.crypto.SecretKey;
import javax.net.ssl.HttpsURLConnection;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

class Fetcher<K,V> extends Thread {
  
  private static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);
  
  /** Number of ms before timing out a copy */
  private static final int DEFAULT_STALLED_COPY_TIMEOUT = 3 * 60 * 1000;
  
  /** Basic/unit connection timeout (in milliseconds) */
  private final static int UNIT_CONNECT_TIMEOUT = 60 * 1000;
  
  /* Default read timeout (in milliseconds) */
  private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;

  // This should be kept in sync with ShuffleHandler.FETCH_RETRY_DELAY.
  private static final long FETCH_RETRY_DELAY_DEFAULT = 1000L;
  static final int TOO_MANY_REQ_STATUS_CODE = 429;
  private static final String FETCH_RETRY_AFTER_HEADER = "Retry-After";

  protected final Reporter reporter;
  private enum ShuffleErrors{IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP,
                                    CONNECTION, WRONG_REDUCE}
  
  private final static String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";
  private final JobConf jobConf;
  private final Counters.Counter connectionErrs;
  private final Counters.Counter ioErrs;
  private final Counters.Counter wrongLengthErrs;
  private final Counters.Counter badIdErrs;
  private final Counters.Counter wrongMapErrs;
  private final Counters.Counter wrongReduceErrs;
  protected final MergeManager<K,V> merger;
  protected final ShuffleSchedulerImpl<K,V> scheduler;
  protected final ShuffleClientMetrics metrics;
  protected final ExceptionReporter exceptionReporter;
  protected final int id;
  private static int nextId = 0;
  protected final int reduce;
  
  private final int connectionTimeout;
  private final int readTimeout;
  
  private final int fetchRetryTimeout;
  private final int fetchRetryInterval;
  
  private final boolean fetchRetryEnabled;
  
  private final SecretKey shuffleSecretKey;

  protected HttpURLConnection connection;
  private volatile boolean stopped = false;
  
  // Initiative value is 0, which means it hasn't retried yet.
  private long retryStartTime = 0;

  private static boolean sslShuffle;
  private static SSLFactory sslFactory;

  public Fetcher(JobConf job, TaskAttemptID reduceId, 
                 ShuffleSchedulerImpl<K,V> scheduler, MergeManager<K,V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter, SecretKey shuffleKey) {
    this(job, reduceId, scheduler, merger, reporter, metrics,
        exceptionReporter, shuffleKey, ++nextId);
  }

  @VisibleForTesting
  Fetcher(JobConf job, TaskAttemptID reduceId, 
                 ShuffleSchedulerImpl<K,V> scheduler, MergeManager<K,V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter, SecretKey shuffleKey,
                 int id) {
    this.jobConf = job;
    this.reporter = reporter;
    this.scheduler = scheduler;
    this.merger = merger;
    this.metrics = metrics;
    this.exceptionReporter = exceptionReporter;
    this.id = id;
    this.reduce = reduceId.getTaskID().getId();
    this.shuffleSecretKey = shuffleKey;
    ioErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.IO_ERROR.toString());
    wrongLengthErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_LENGTH.toString());
    badIdErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.BAD_ID.toString());
    wrongMapErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_MAP.toString());
    connectionErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.CONNECTION.toString());
    wrongReduceErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_REDUCE.toString());
    
    this.connectionTimeout = 
      job.getInt(MRJobConfig.SHUFFLE_CONNECT_TIMEOUT,
                 DEFAULT_STALLED_COPY_TIMEOUT);
    this.readTimeout = 
      job.getInt(MRJobConfig.SHUFFLE_READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
    
    this.fetchRetryInterval = job.getInt(MRJobConfig.SHUFFLE_FETCH_RETRY_INTERVAL_MS,
        MRJobConfig.DEFAULT_SHUFFLE_FETCH_RETRY_INTERVAL_MS);
    
    this.fetchRetryTimeout = job.getInt(MRJobConfig.SHUFFLE_FETCH_RETRY_TIMEOUT_MS, 
        DEFAULT_STALLED_COPY_TIMEOUT);
    
    boolean shuffleFetchEnabledDefault = job.getBoolean(
        YarnConfiguration.NM_RECOVERY_ENABLED, 
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    this.fetchRetryEnabled = job.getBoolean(
        MRJobConfig.SHUFFLE_FETCH_RETRY_ENABLED, 
        shuffleFetchEnabledDefault);
    
    setName("fetcher#" + id);
    setDaemon(true);

    synchronized (Fetcher.class) {
      sslShuffle = job.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
                                  MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT);
      if (sslShuffle && sslFactory == null) {
        sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, job);
        try {
          sslFactory.init();
        } catch (Exception ex) {
          sslFactory.destroy();
          throw new RuntimeException(ex);
        }
      }
    }
  }
  
  public void run() {
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        MapHost host = null;
        try {
          // If merge is on, block
          merger.waitForResource();

          // Get a host to shuffle from
          host = scheduler.getHost();
          metrics.threadBusy();

          // Shuffle
          copyFromHost(host);
        } finally {
          if (host != null) {
            scheduler.freeHost(host);
            metrics.threadFree();            
          }
        }
      }
    } catch (InterruptedException ie) {
      return;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
    }
  }

  @Override
  public void interrupt() {
    try {
      closeConnection();
    } finally {
      super.interrupt();
    }
  }

  public void shutDown() throws InterruptedException {
    this.stopped = true;
    interrupt();
    try {
      join(5000);
    } catch (InterruptedException ie) {
      LOG.warn("Got interrupt while joining " + getName(), ie);
    }
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  @VisibleForTesting
  protected synchronized void openConnection(URL url)
      throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    if (sslShuffle) {
      HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
      try {
        httpsConn.setSSLSocketFactory(sslFactory.createSSLSocketFactory());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
      httpsConn.setHostnameVerifier(sslFactory.getHostnameVerifier());
    }
    connection = conn;
  }

  protected synchronized void closeConnection() {
    // Note that HttpURLConnection::disconnect() doesn't trash the object.
    // connect() attempts to reconnect in a loop, possibly reversing this
    if (connection != null) {
      connection.disconnect();
    }
  }

  private void abortConnect(MapHost host, Set<TaskAttemptID> remaining) {
    for (TaskAttemptID left : remaining) {
      scheduler.putBackKnownMapOutput(host, left);
    }
    closeConnection();
  }

  private DataInputStream openShuffleUrl(MapHost host,
      Set<TaskAttemptID> remaining, URL url) {
    DataInputStream input = null;

    try {
      setupConnectionsWithRetry(url);
      if (stopped) {
        abortConnect(host, remaining);
      } else {
        input = new DataInputStream(connection.getInputStream());
      }
    } catch (TryAgainLaterException te) {
      LOG.warn("Connection rejected by the host " + te.host +
          ". Will retry later.");
      scheduler.penalize(host, te.backoff);
    } catch (IOException ie) {
      boolean connectExcpt = ie instanceof ConnectException;
      ioErrs.increment(1);
      LOG.warn("Failed to connect to " + host + " with " + remaining.size() +
               " map outputs", ie);

      // If connect did not succeed, just mark all the maps as failed,
      // indirectly penalizing the host
      scheduler.hostFailed(host.getHostName());
      for(TaskAttemptID left: remaining) {
        scheduler.copyFailed(left, host, false, connectExcpt);
      }
    }

    return input;
  }

  /**
   * The crux of the matter...
   * 
   * @param host {@link MapHost} from which we need to  
   *              shuffle available map-outputs.
   */
  @VisibleForTesting
  protected void copyFromHost(MapHost host) throws IOException {
    // reset retryStartTime for a new host
    retryStartTime = 0;
    // Get completed maps on 'host'
    List<TaskAttemptID> maps = scheduler.getMapsForHost(host);
    
    // Sanity check to catch hosts with only 'OBSOLETE' maps, 
    // especially at the tail of large jobs
    if (maps.size() == 0) {
      return;
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + id + " going to fetch from " + host + " for: "
        + maps);
    }
    
    // List of maps to be fetched yet
    Set<TaskAttemptID> remaining = new HashSet<TaskAttemptID>(maps);
    
    // Construct the url and connect
    URL url = getMapOutputURL(host, maps);
    DataInputStream input = null;
    
    try {
      input = openShuffleUrl(host, remaining, url);
      if (input == null) {
        return;
      }

      // Loop through available map-outputs and fetch them
      // On any error, faildTasks is not null and we exit
      // after putting back the remaining maps to the 
      // yet_to_be_fetched list and marking the failed tasks.
      TaskAttemptID[] failedTasks = null;
      while (!remaining.isEmpty() && failedTasks == null) {
        try {
          failedTasks = copyMapOutput(host, input, remaining, fetchRetryEnabled);
        } catch (IOException e) {
          IOUtils.cleanupWithLogger(LOG, input);
          //
          // Setup connection again if disconnected by NM
          connection.disconnect();
          // Get map output from remaining tasks only.
          url = getMapOutputURL(host, remaining);
          input = openShuffleUrl(host, remaining, url);
          if (input == null) {
            return;
          }
        }
      }
      
      if(failedTasks != null && failedTasks.length > 0) {
        LOG.warn("copyMapOutput failed for tasks "+Arrays.toString(failedTasks));
        scheduler.hostFailed(host.getHostName());
        for(TaskAttemptID left: failedTasks) {
          scheduler.copyFailed(left, host, true, false);
        }
      }

      // Sanity check
      if (failedTasks == null && !remaining.isEmpty()) {
        throw new IOException("server didn't return all expected map outputs: "
            + remaining.size() + " left.");
      }
      input.close();
      input = null;
    } finally {
      if (input != null) {
        IOUtils.cleanupWithLogger(LOG, input);
        input = null;
      }
      for (TaskAttemptID left : remaining) {
        scheduler.putBackKnownMapOutput(host, left);
      }
    }
  }

  private void setupConnectionsWithRetry(URL url) throws IOException {
    openConnectionWithRetry(url);
    if (stopped) {
      return;
    }
      
    // generate hash of the url
    String msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
    String encHash = SecureShuffleUtils.hashFromString(msgToEncode,
        shuffleSecretKey);
    
    setupShuffleConnection(encHash);
    connect(connection, connectionTimeout);
    // verify that the thread wasn't stopped during calls to connect
    if (stopped) {
      return;
    }
    
    verifyConnection(url, msgToEncode, encHash);
  }

  private void openConnectionWithRetry(URL url) throws IOException {
    long startTime = Time.monotonicNow();
    boolean shouldWait = true;
    while (shouldWait) {
      try {
        openConnection(url);
        shouldWait = false;
      } catch (IOException e) {
        if (!fetchRetryEnabled) {
           // throw exception directly if fetch's retry is not enabled
           throw e;
        }
        if ((Time.monotonicNow() - startTime) >= this.fetchRetryTimeout) {
          LOG.warn("Failed to connect to host: " + url + "after " 
              + fetchRetryTimeout + " milliseconds.");
          throw e;
        }
        try {
          Thread.sleep(this.fetchRetryInterval);
        } catch (InterruptedException e1) {
          if (stopped) {
            return;
          }
        }
      }
    }
  }

  private void verifyConnection(URL url, String msgToEncode, String encHash)
      throws IOException {
    // Validate response code
    int rc = connection.getResponseCode();
    // See if the shuffleHandler rejected the connection due to too many
    // reducer requests. If so, signal fetchers to back off.
    if (rc == TOO_MANY_REQ_STATUS_CODE) {
      long backoff = connection.getHeaderFieldLong(FETCH_RETRY_AFTER_HEADER,
          FETCH_RETRY_DELAY_DEFAULT);
      // in case we get a negative backoff from ShuffleHandler
      if (backoff < 0) {
        backoff = FETCH_RETRY_DELAY_DEFAULT;
        LOG.warn("Get a negative backoff value from ShuffleHandler. Setting" +
            " it to the default value " + FETCH_RETRY_DELAY_DEFAULT);
      }
      throw new TryAgainLaterException(backoff, url.getHost());
    }
    if (rc != HttpURLConnection.HTTP_OK) {
      throw new IOException(
          "Got invalid response code " + rc + " from " + url +
          ": " + connection.getResponseMessage());
    }
    // get the shuffle version
    if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(
        connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(
            connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))) {
      throw new IOException("Incompatible shuffle response version");
    }
    // get the replyHash which is HMac of the encHash we sent to the server
    String replyHash = connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
    if(replyHash==null) {
      throw new IOException("security validation of TT Map output failed");
    }
    LOG.debug("url="+msgToEncode+";encHash="+encHash+";replyHash="+replyHash);
    // verify that replyHash is HMac of encHash
    SecureShuffleUtils.verifyReply(replyHash, encHash, shuffleSecretKey);
    LOG.debug("for url="+msgToEncode+" sent hash and received reply");
  }

  private void setupShuffleConnection(String encHash) {
    // put url hash into http header
    connection.addRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    // set the read timeout
    connection.setReadTimeout(readTimeout);
    // put shuffle version into http header
    connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
  }
  
  private static TaskAttemptID[] EMPTY_ATTEMPT_ID_ARRAY = new TaskAttemptID[0];
  
  private TaskAttemptID[] copyMapOutput(MapHost host,
                                DataInputStream input,
                                Set<TaskAttemptID> remaining,
                                boolean canRetry) throws IOException {
    MapOutput<K,V> mapOutput = null;
    TaskAttemptID mapId = null;
    long decompressedLength = -1;
    long compressedLength = -1;
    
    try {
      long startTime = Time.monotonicNow();
      int forReduce = -1;
      //Read the shuffle header
      try {
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(input);
        mapId = TaskAttemptID.forName(header.mapId);
        compressedLength = header.compressedLength;
        decompressedLength = header.uncompressedLength;
        forReduce = header.forReduce;
      } catch (IllegalArgumentException e) {
        badIdErrs.increment(1);
        LOG.warn("Invalid map id ", e);
        //Don't know which one was bad, so consider all of them as bad
        return remaining.toArray(new TaskAttemptID[remaining.size()]);
      }

      InputStream is = input;
      is =
          IntermediateEncryptedStream.wrapIfNecessary(jobConf, is,
              compressedLength, null);
      compressedLength -= CryptoUtils.cryptoPadding(jobConf);
      decompressedLength -= CryptoUtils.cryptoPadding(jobConf);
      
      // Do some basic sanity verification
      if (!verifySanity(compressedLength, decompressedLength, forReduce,
          remaining, mapId)) {
        return new TaskAttemptID[] {mapId};
      }
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("header: " + mapId + ", len: " + compressedLength + 
            ", decomp len: " + decompressedLength);
      }
      
      // Get the location for the map output - either in-memory or on-disk
      try {
        mapOutput = merger.reserve(mapId, decompressedLength, id);
      } catch (IOException ioe) {
        // kill this reduce attempt
        ioErrs.increment(1);
        scheduler.reportLocalError(ioe);
        return EMPTY_ATTEMPT_ID_ARRAY;
      }
      
      // Check if we can shuffle *now* ...
      if (mapOutput == null) {
        LOG.info("fetcher#" + id + " - MergeManager returned status WAIT ...");
        //Not an error but wait to process data.
        return EMPTY_ATTEMPT_ID_ARRAY;
      } 
      
      // The codec for lz0,lz4,snappy,bz2,etc. throw java.lang.InternalError
      // on decompression failures. Catching and re-throwing as IOException
      // to allow fetch failure logic to be processed
      try {
        // Go!
        LOG.info("fetcher#" + id + " about to shuffle output of map "
            + mapOutput.getMapId() + " decomp: " + decompressedLength
            + " len: " + compressedLength + " to " + mapOutput.getDescription());
        mapOutput.shuffle(host, is, compressedLength, decompressedLength,
            metrics, reporter);
      } catch (java.lang.InternalError | Exception e) {
        LOG.warn("Failed to shuffle for fetcher#"+id, e);
        throw new IOException(e);
      }
      
      // Inform the shuffle scheduler
      long endTime = Time.monotonicNow();
      // Reset retryStartTime as map task make progress if retried before.
      retryStartTime = 0;
      
      scheduler.copySucceeded(mapId, host, compressedLength, 
                              startTime, endTime, mapOutput);
      // Note successful shuffle
      remaining.remove(mapId);
      metrics.successFetch();
      return null;
    } catch (IOException ioe) {
      if (mapOutput != null) {
        mapOutput.abort();
      }

      if (canRetry) {
        checkTimeoutOrRetry(host, ioe);
      } 
      
      ioErrs.increment(1);
      if (mapId == null || mapOutput == null) {
        LOG.warn("fetcher#" + id + " failed to read map header" + 
                 mapId + " decomp: " + 
                 decompressedLength + ", " + compressedLength, ioe);
        if(mapId == null) {
          return remaining.toArray(new TaskAttemptID[remaining.size()]);
        } else {
          return new TaskAttemptID[] {mapId};
        }
      }
        
      LOG.warn("Failed to shuffle output of " + mapId + 
               " from " + host.getHostName(), ioe); 

      // Inform the shuffle-scheduler
      metrics.failedFetch();
      return new TaskAttemptID[] {mapId};
    }

  }

  /** check if hit timeout of retry, if not, throw an exception and start a 
   *  new round of retry.*/
  private void checkTimeoutOrRetry(MapHost host, IOException ioe)
      throws IOException {
    // First time to retry.
    long currentTime = Time.monotonicNow();
    if (retryStartTime == 0) {
       retryStartTime = currentTime;
    }
  
    // Retry is not timeout, let's do retry with throwing an exception.
    if (currentTime - retryStartTime < this.fetchRetryTimeout) {
      LOG.warn("Shuffle output from " + host.getHostName() +
          " failed, retry it.", ioe);
      throw ioe;
    } else {
      // timeout, prepare to be failed.
      LOG.warn("Timeout for copying MapOutput with retry on host " + host 
          + "after " + fetchRetryTimeout + " milliseconds.");
      
    }
  }
  
  /**
   * Do some basic verification on the input received -- Being defensive
   * @param compressedLength
   * @param decompressedLength
   * @param forReduce
   * @param remaining
   * @param mapId
   * @return true/false, based on if the verification succeeded or not
   */
  private boolean verifySanity(long compressedLength, long decompressedLength,
      int forReduce, Set<TaskAttemptID> remaining, TaskAttemptID mapId) {
    if (compressedLength < 0 || decompressedLength < 0) {
      wrongLengthErrs.increment(1);
      LOG.warn(getName() + " invalid lengths in map output header: id: " +
               mapId + " len: " + compressedLength + ", decomp len: " + 
               decompressedLength);
      return false;
    }
    
    if (forReduce != reduce) {
      wrongReduceErrs.increment(1);
      LOG.warn(getName() + " data for the wrong reduce map: " +
               mapId + " len: " + compressedLength + " decomp len: " +
               decompressedLength + " for reduce " + forReduce);
      return false;
    }

    // Sanity check
    if (!remaining.contains(mapId)) {
      wrongMapErrs.increment(1);
      LOG.warn("Invalid map-output! Received output for " + mapId);
      return false;
    }
    
    return true;
  }

  /**
   * Create the map-output-url. This will contain all the map ids
   * separated by commas
   * @param host
   * @param maps
   * @return
   * @throws MalformedURLException
   */
  private URL getMapOutputURL(MapHost host, Collection<TaskAttemptID> maps
                              )  throws MalformedURLException {
    // Get the base url
    StringBuffer url = new StringBuffer(host.getBaseUrl());
    
    boolean first = true;
    for (TaskAttemptID mapId : maps) {
      if (!first) {
        url.append(",");
      }
      url.append(mapId);
      first = false;
    }
   
    LOG.debug("MapOutput URL for " + host + " -> " + url.toString());
    return new URL(url.toString());
  }
  
  /** 
   * The connection establishment is attempted multiple times and is given up 
   * only on the last failure. Instead of connecting with a timeout of 
   * X, we try connecting with a timeout of x < X but multiple times. 
   */
  private void connect(URLConnection connection, int connectionTimeout)
  throws IOException {
    int unit = 0;
    if (connectionTimeout < 0) {
      throw new IOException("Invalid timeout "
                            + "[timeout = " + connectionTimeout + " ms]");
    } else if (connectionTimeout > 0) {
      unit = Math.min(UNIT_CONNECT_TIMEOUT, connectionTimeout);
    }
    long startTime = Time.monotonicNow();
    long lastTime = startTime;
    int attempts = 0;
    // set the connect timeout to the unit-connect-timeout
    connection.setConnectTimeout(unit);
    while (true) {
      try {
        attempts++;
        connection.connect();
        break;
      } catch (IOException ioe) {
        long currentTime = Time.monotonicNow();
        long retryTime = currentTime - startTime;
        long leftTime = connectionTimeout - retryTime;
        long timeSinceLastIteration = currentTime - lastTime;
        // throw an exception if we have waited for timeout amount of time
        // note that the updated value if timeout is used here
        if (leftTime <= 0) {
          int retryTimeInSeconds = (int) retryTime/1000;
          LOG.error("Connection retry failed with " + attempts + 
              " attempts in " + retryTimeInSeconds + " seconds");
          throw ioe;
        }
        // reset the connect timeout for the last try
        if (leftTime < unit) {
          unit = (int)leftTime;
          // reset the connect time out for the final connect
          connection.setConnectTimeout(unit);
        }
        
        if (timeSinceLastIteration < unit) {
          try {
            // sleep the left time of unit
            sleep(unit - timeSinceLastIteration);
          } catch (InterruptedException e) {
            LOG.warn("Sleep in connection retry get interrupted.");
            if (stopped) {
              return;
            }
          }
        }
        // update the total remaining connect-timeout
        lastTime = Time.monotonicNow();
      }
    }
  }

  private static class TryAgainLaterException extends IOException {
    public final long backoff;
    public final String host;

    public TryAgainLaterException(long backoff, String host) {
      super("Too many requests to a map host");
      this.backoff = backoff;
      this.host = host;
    }
  }
}
