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

package org.apache.hadoop.mapred;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.crypto.SecretKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.proto.ShuffleHandlerRecoveryProtos.JobShuffleInfoProto;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;

public class ShuffleHandler extends AuxiliaryService {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(ShuffleHandler.class);
  private static final org.slf4j.Logger AUDITLOG =
      LoggerFactory.getLogger(ShuffleHandler.class.getName()+".audit");
  public static final String SHUFFLE_MANAGE_OS_CACHE = "mapreduce.shuffle.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_READAHEAD_BYTES = "mapreduce.shuffle.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;
  
  // pattern to identify errors related to the client closing the socket early
  // idea borrowed from Netty SslHandler
  private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
      "^.*(?:connection.*reset|connection.*closed|broken.*pipe).*$",
      Pattern.CASE_INSENSITIVE);

  private static final String STATE_DB_NAME = "mapreduce_shuffle_state";
  private static final String STATE_DB_SCHEMA_VERSION_KEY = "shuffle-schema-version";
  protected static final Version CURRENT_VERSION_INFO = 
      Version.newInstance(1, 0);

  private static final String DATA_FILE_NAME = "file.out";
  private static final String INDEX_FILE_NAME = "file.out.index";

  public static final HttpResponseStatus TOO_MANY_REQ_STATUS =
      new HttpResponseStatus(429, "TOO MANY REQUESTS");
  // This should kept in sync with Fetcher.FETCH_RETRY_DELAY_DEFAULT
  public static final long FETCH_RETRY_DELAY = 1000L;
  public static final String RETRY_AFTER_HEADER = "Retry-After";

  private int port;
  private ChannelFactory selector;
  private final ChannelGroup accepted = new DefaultChannelGroup();
  protected HttpPipelineFactory pipelineFact;
  private int sslFileBufferSize;
  
  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile
   */
  private boolean manageOsCache;
  private int readaheadLength;
  private int maxShuffleConnections;
  private int shuffleBufferSize;
  private boolean shuffleTransferToAllowed;
  private int maxSessionOpenFiles;
  private ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  private Map<String,String> userRsrc;
  private JobTokenSecretManager secretManager;

  private DB stateDb = null;

  public static final String MAPREDUCE_SHUFFLE_SERVICEID =
      "mapreduce_shuffle";

  public static final String SHUFFLE_PORT_CONFIG_KEY = "mapreduce.shuffle.port";
  public static final int DEFAULT_SHUFFLE_PORT = 13562;

  public static final String SHUFFLE_LISTEN_QUEUE_SIZE =
      "mapreduce.shuffle.listen.queue.size";
  public static final int DEFAULT_SHUFFLE_LISTEN_QUEUE_SIZE = 128;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED =
      "mapreduce.shuffle.connection-keep-alive.enable";
  public static final boolean DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED = false;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT =
      "mapreduce.shuffle.connection-keep-alive.timeout";
  public static final int DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT = 5; //seconds

  public static final String SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      "mapreduce.shuffle.mapoutput-info.meta.cache.size";
  public static final int DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      1000;

  public static final String CONNECTION_CLOSE = "close";

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
    "mapreduce.shuffle.ssl.file.buffer.size";

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

  public static final String MAX_SHUFFLE_CONNECTIONS = "mapreduce.shuffle.max.connections";
  public static final int DEFAULT_MAX_SHUFFLE_CONNECTIONS = 0; // 0 implies no limit
  
  public static final String MAX_SHUFFLE_THREADS = "mapreduce.shuffle.max.threads";
  // 0 implies Netty default of 2 * number of available processors
  public static final int DEFAULT_MAX_SHUFFLE_THREADS = 0;
  
  public static final String SHUFFLE_BUFFER_SIZE = 
      "mapreduce.shuffle.transfer.buffer.size";
  public static final int DEFAULT_SHUFFLE_BUFFER_SIZE = 128 * 1024;
  
  public static final String  SHUFFLE_TRANSFERTO_ALLOWED = 
      "mapreduce.shuffle.transferTo.allowed";
  public static final boolean DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED = true;
  public static final boolean WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED = 
      false;
  private static final String TIMEOUT_HANDLER = "timeout";

  /* the maximum number of files a single GET request can
   open simultaneously during shuffle
   */
  public static final String SHUFFLE_MAX_SESSION_OPEN_FILES =
      "mapreduce.shuffle.max.session-open-files";
  public static final int DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES = 3;

  boolean connectionKeepAliveEnabled = false;
  private int connectionKeepAliveTimeOut;
  private int mapOutputMetaInfoCacheSize;
  private Timer timer;

  @Metrics(about="Shuffle output metrics", context="mapred")
  static class ShuffleMetrics implements ChannelFutureListener {
    @Metric("Shuffle output in bytes")
        MutableCounterLong shuffleOutputBytes;
    @Metric("# of failed shuffle outputs")
        MutableCounterInt shuffleOutputsFailed;
    @Metric("# of succeeeded shuffle outputs")
        MutableCounterInt shuffleOutputsOK;
    @Metric("# of current shuffle connections")
        MutableGaugeInt shuffleConnections;

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        shuffleOutputsOK.incr();
      } else {
        shuffleOutputsFailed.incr();
      }
      shuffleConnections.decr();
    }
  }

  final ShuffleMetrics metrics;

  class ReduceMapFileCount implements ChannelFutureListener {

    private ReduceContext reduceContext;

    public ReduceMapFileCount(ReduceContext rc) {
      this.reduceContext = rc;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (!future.isSuccess()) {
        future.getChannel().close();
        return;
      }
      int waitCount = this.reduceContext.getMapsToWait().decrementAndGet();
      if (waitCount == 0) {
        metrics.operationComplete(future);
        // Let the idle timer handler close keep-alive connections
        if (reduceContext.getKeepAlive()) {
          ChannelPipeline pipeline = future.getChannel().getPipeline();
          TimeoutHandler timeoutHandler =
              (TimeoutHandler)pipeline.get(TIMEOUT_HANDLER);
          timeoutHandler.setEnabledTimeout(true);
        } else {
          future.getChannel().close();
        }
      } else {
        pipelineFact.getSHUFFLE().sendMap(reduceContext);
      }
    }
  }

  /**
   * Maintain parameters per messageReceived() Netty context.
   * Allows sendMapOutput calls from operationComplete()
   */
  private static class ReduceContext {

    private List<String> mapIds;
    private AtomicInteger mapsToWait;
    private AtomicInteger mapsToSend;
    private int reduceId;
    private ChannelHandlerContext ctx;
    private String user;
    private Map<String, Shuffle.MapOutputInfo> infoMap;
    private String jobId;
    private final boolean keepAlive;

    public ReduceContext(List<String> mapIds, int rId,
                         ChannelHandlerContext context, String usr,
                         Map<String, Shuffle.MapOutputInfo> mapOutputInfoMap,
                         String jobId, boolean keepAlive) {

      this.mapIds = mapIds;
      this.reduceId = rId;
      /**
      * Atomic count for tracking the no. of map outputs that are yet to
      * complete. Multiple futureListeners' operationComplete() can decrement
      * this value asynchronously. It is used to decide when the channel should
      * be closed.
      */
      this.mapsToWait = new AtomicInteger(mapIds.size());
      /**
      * Atomic count for tracking the no. of map outputs that have been sent.
      * Multiple sendMap() calls can increment this value
      * asynchronously. Used to decide which mapId should be sent next.
      */
      this.mapsToSend = new AtomicInteger(0);
      this.ctx = context;
      this.user = usr;
      this.infoMap = mapOutputInfoMap;
      this.jobId = jobId;
      this.keepAlive = keepAlive;
    }

    public int getReduceId() {
      return reduceId;
    }

    public ChannelHandlerContext getCtx() {
      return ctx;
    }

    public String getUser() {
      return user;
    }

    public Map<String, Shuffle.MapOutputInfo> getInfoMap() {
      return infoMap;
    }

    public String getJobId() {
      return jobId;
    }

    public List<String> getMapIds() {
      return mapIds;
    }

    public AtomicInteger getMapsToSend() {
      return mapsToSend;
    }

    public AtomicInteger getMapsToWait() {
      return mapsToWait;
    }

    public boolean getKeepAlive() {
      return keepAlive;
    }
  }

  ShuffleHandler(MetricsSystem ms) {
    super(MAPREDUCE_SHUFFLE_SERVICEID);
    metrics = ms.register(new ShuffleMetrics());
  }

  public ShuffleHandler() {
    this(DefaultMetricsSystem.instance());
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * @param port the port to be sent to the ApplciationMaster
   * @return the serialized form of the port.
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer port_dob = new DataOutputBuffer();
    port_dob.writeInt(port);
    return ByteBuffer.wrap(port_dob.getData(), 0, port_dob.getLength());
  }

  /**
   * A helper function to deserialize the metadata returned by ShuffleHandler.
   * @param meta the metadata returned by the ShuffleHandler
   * @return the port the Shuffle Handler is listening on to serve shuffle data.
   */
  public static int deserializeMetaData(ByteBuffer meta) throws IOException {
    //TODO this should be returning a class not just an int
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    int port = in.readInt();
    return port;
  }

  /**
   * A helper function to serialize the JobTokenIdentifier to be sent to the
   * ShuffleHandler as ServiceData.
   * @param jobToken the job token to be used for authentication of
   * shuffle data requests.
   * @return the serialized version of the jobToken.
   */
  public static ByteBuffer serializeServiceData(Token<JobTokenIdentifier> jobToken) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer jobToken_dob = new DataOutputBuffer();
    jobToken.write(jobToken_dob);
    return ByteBuffer.wrap(jobToken_dob.getData(), 0, jobToken_dob.getLength());
  }

  static Token<JobTokenIdentifier> deserializeServiceData(ByteBuffer secret) throws IOException {
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(secret);
    Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>();
    jt.readFields(in);
    return jt;
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {

    String user = context.getUser();
    ApplicationId appId = context.getApplicationId();
    ByteBuffer secret = context.getApplicationDataForService();
    // TODO these bytes should be versioned
    try {
      Token<JobTokenIdentifier> jt = deserializeServiceData(secret);
       // TODO: Once SHuffle is out of NM, this can use MR APIs
      JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
      recordJobShuffleInfo(jobId, user, jt);
    } catch (IOException e) {
      LOG.error("Error during initApp", e);
      // TODO add API to AuxiliaryServices to report failures
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    ApplicationId appId = context.getApplicationId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    try {
      removeJobShuffleInfo(jobId);
    } catch (IOException e) {
      LOG.error("Error during stopApp", e);
      // TODO add API to AuxiliaryServices to report failures
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    manageOsCache = conf.getBoolean(SHUFFLE_MANAGE_OS_CACHE,
        DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

    readaheadLength = conf.getInt(SHUFFLE_READAHEAD_BYTES,
        DEFAULT_SHUFFLE_READAHEAD_BYTES);
    
    maxShuffleConnections = conf.getInt(MAX_SHUFFLE_CONNECTIONS, 
                                        DEFAULT_MAX_SHUFFLE_CONNECTIONS);
    int maxShuffleThreads = conf.getInt(MAX_SHUFFLE_THREADS,
                                        DEFAULT_MAX_SHUFFLE_THREADS);
    if (maxShuffleThreads == 0) {
      maxShuffleThreads = 2 * Runtime.getRuntime().availableProcessors();
    }
    
    shuffleBufferSize = conf.getInt(SHUFFLE_BUFFER_SIZE, 
                                    DEFAULT_SHUFFLE_BUFFER_SIZE);
        
    shuffleTransferToAllowed = conf.getBoolean(SHUFFLE_TRANSFERTO_ALLOWED,
         (Shell.WINDOWS)?WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED:
                         DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED);

    maxSessionOpenFiles = conf.getInt(SHUFFLE_MAX_SESSION_OPEN_FILES,
        DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES);

    ThreadFactory bossFactory = new ThreadFactoryBuilder()
      .setNameFormat("ShuffleHandler Netty Boss #%d")
      .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
      .setNameFormat("ShuffleHandler Netty Worker #%d")
      .build();
    
    selector = new NioServerSocketChannelFactory(
        HadoopExecutors.newCachedThreadPool(bossFactory),
        HadoopExecutors.newCachedThreadPool(workerFactory),
        maxShuffleThreads);
    super.serviceInit(new Configuration(conf));
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    userRsrc = new ConcurrentHashMap<String,String>();
    secretManager = new JobTokenSecretManager();
    recoverState(conf);
    ServerBootstrap bootstrap = new ServerBootstrap(selector);
    // Timer is shared across entire factory and must be released separately
    timer = new HashedWheelTimer();
    try {
      pipelineFact = new HttpPipelineFactory(conf, timer);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    bootstrap.setOption("backlog", conf.getInt(SHUFFLE_LISTEN_QUEUE_SIZE,
        DEFAULT_SHUFFLE_LISTEN_QUEUE_SIZE));
    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setPipelineFactory(pipelineFact);
    port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    Channel ch = bootstrap.bind(new InetSocketAddress(port));
    accepted.add(ch);
    port = ((InetSocketAddress)ch.getLocalAddress()).getPort();
    conf.set(SHUFFLE_PORT_CONFIG_KEY, Integer.toString(port));
    pipelineFact.SHUFFLE.setPort(port);
    LOG.info(getName() + " listening on port " + port);
    super.serviceStart();

    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
                                    DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);
    connectionKeepAliveEnabled =
        conf.getBoolean(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED,
          DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED);
    connectionKeepAliveTimeOut =
        Math.max(1, conf.getInt(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
          DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT));
    mapOutputMetaInfoCacheSize =
        Math.max(1, conf.getInt(SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE,
          DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE));
  }

  @Override
  protected void serviceStop() throws Exception {
    accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    if (selector != null) {
      ServerBootstrap bootstrap = new ServerBootstrap(selector);
      bootstrap.releaseExternalResources();
    }
    if (pipelineFact != null) {
      pipelineFact.destroy();
    }
    if (timer != null) {
      // Release this shared timer resource
      timer.stop();
    }
    if (stateDb != null) {
      stateDb.close();
    }
    super.serviceStop();
  }

  @Override
  public synchronized ByteBuffer getMetaData() {
    try {
      return serializeMetaData(port); 
    } catch (IOException e) {
      LOG.error("Error during getMeta", e);
      // TODO add API to AuxiliaryServices to report failures
      return null;
    }
  }

  protected Shuffle getShuffle(Configuration conf) {
    return new Shuffle(conf);
  }

  private void recoverState(Configuration conf) throws IOException {
    Path recoveryRoot = getRecoveryPath();
    if (recoveryRoot != null) {
      startStore(recoveryRoot);
      Pattern jobPattern = Pattern.compile(JobID.JOBID_REGEX);
      LeveldbIterator iter = null;
      try {
        iter = new LeveldbIterator(stateDb);
        iter.seek(bytes(JobID.JOB));
        while (iter.hasNext()) {
          Map.Entry<byte[],byte[]> entry = iter.next();
          String key = asString(entry.getKey());
          if (!jobPattern.matcher(key).matches()) {
            break;
          }
          recoverJobShuffleInfo(key, entry.getValue());
        }
      } catch (DBException e) {
        throw new IOException("Database error during recovery", e);
      } finally {
        if (iter != null) {
          iter.close();
        }
      }
    }
  }

  private void startStore(Path recoveryRoot) throws IOException {
    Options options = new Options();
    options.createIfMissing(false);
    Path dbPath = new Path(recoveryRoot, STATE_DB_NAME);
    LOG.info("Using state database at " + dbPath + " for recovery");
    File dbfile = new File(dbPath.toString());
    try {
      stateDb = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating state database at " + dbfile);
        options.createIfMissing(true);
        try {
          stateDb = JniDBFactory.factory.open(dbfile, options);
          storeVersion();
        } catch (DBException dbExc) {
          throw new IOException("Unable to create state store", dbExc);
        }
      } else {
        throw e;
      }
    }
    checkVersion();
  }
  
  @VisibleForTesting
  Version loadVersion() throws IOException {
    byte[] data = stateDb.get(bytes(STATE_DB_SCHEMA_VERSION_KEY));
    // if version is not stored previously, treat it as CURRENT_VERSION_INFO.
    if (data == null || data.length == 0) {
      return getCurrentVersion();
    }
    Version version =
        new VersionPBImpl(VersionProto.parseFrom(data));
    return version;
  }

  private void storeSchemaVersion(Version version) throws IOException {
    String key = STATE_DB_SCHEMA_VERSION_KEY;
    byte[] data = 
        ((VersionPBImpl) version).getProto().toByteArray();
    try {
      stateDb.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
  
  private void storeVersion() throws IOException {
    storeSchemaVersion(CURRENT_VERSION_INFO);
  }
  
  // Only used for test
  @VisibleForTesting
  void storeVersion(Version version) throws IOException {
    storeSchemaVersion(version);
  }

  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }
  
  /**
   * 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of DB schema is a major upgrade, and any
   *    compatible change of DB schema is a minor upgrade.
   * 3) Within a minor upgrade, say 1.1 to 1.2:
   *    overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0:
   *    throw exception and indicate user to use a separate upgrade tool to
   *    upgrade shuffle info or remove incompatible old state.
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded state DB schema version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing state DB schema version info " + getCurrentVersion());
      storeVersion();
    } else {
      throw new IOException(
        "Incompatible version for state DB schema: expecting DB schema version " 
            + getCurrentVersion() + ", but loading version " + loadedVersion);
    }
  }

  private void addJobToken(JobID jobId, String user,
      Token<JobTokenIdentifier> jobToken) {
    userRsrc.put(jobId.toString(), user);
    secretManager.addTokenForJob(jobId.toString(), jobToken);
    LOG.info("Added token for " + jobId.toString());
  }

  private void recoverJobShuffleInfo(String jobIdStr, byte[] data)
      throws IOException {
    JobID jobId;
    try {
      jobId = JobID.forName(jobIdStr);
    } catch (IllegalArgumentException e) {
      throw new IOException("Bad job ID " + jobIdStr + " in state store", e);
    }

    JobShuffleInfoProto proto = JobShuffleInfoProto.parseFrom(data);
    String user = proto.getUser();
    TokenProto tokenProto = proto.getJobToken();
    Token<JobTokenIdentifier> jobToken = new Token<JobTokenIdentifier>(
        tokenProto.getIdentifier().toByteArray(),
        tokenProto.getPassword().toByteArray(),
        new Text(tokenProto.getKind()), new Text(tokenProto.getService()));
    addJobToken(jobId, user, jobToken);
  }

  private void recordJobShuffleInfo(JobID jobId, String user,
      Token<JobTokenIdentifier> jobToken) throws IOException {
    if (stateDb != null) {
      TokenProto tokenProto = TokenProto.newBuilder()
          .setIdentifier(ByteString.copyFrom(jobToken.getIdentifier()))
          .setPassword(ByteString.copyFrom(jobToken.getPassword()))
          .setKind(jobToken.getKind().toString())
          .setService(jobToken.getService().toString())
          .build();
      JobShuffleInfoProto proto = JobShuffleInfoProto.newBuilder()
          .setUser(user).setJobToken(tokenProto).build();
      try {
        stateDb.put(bytes(jobId.toString()), proto.toByteArray());
      } catch (DBException e) {
        throw new IOException("Error storing " + jobId, e);
      }
    }
    addJobToken(jobId, user, jobToken);
  }

  private void removeJobShuffleInfo(JobID jobId) throws IOException {
    String jobIdStr = jobId.toString();
    secretManager.removeTokenForJob(jobIdStr);
    userRsrc.remove(jobIdStr);
    if (stateDb != null) {
      try {
        stateDb.delete(bytes(jobIdStr));
      } catch (DBException e) {
        throw new IOException("Unable to remove " + jobId
            + " from state store", e);
      }
    }
  }

  static class TimeoutHandler extends IdleStateAwareChannelHandler {

    private boolean enabledTimeout;

    void setEnabledTimeout(boolean enabledTimeout) {
      this.enabledTimeout = enabledTimeout;
    }

    @Override
    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) {
      if (e.getState() == IdleState.WRITER_IDLE && enabledTimeout) {
        e.getChannel().close();
      }
    }
  }

  class HttpPipelineFactory implements ChannelPipelineFactory {

    final Shuffle SHUFFLE;
    private SSLFactory sslFactory;
    private final ChannelHandler idleStateHandler;

    public HttpPipelineFactory(Configuration conf, Timer timer) throws Exception {
      SHUFFLE = getShuffle(conf);
      if (conf.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
                          MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT)) {
        LOG.info("Encrypted shuffle is enabled.");
        sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
        sslFactory.init();
      }
      this.idleStateHandler = new IdleStateHandler(timer, 0, connectionKeepAliveTimeOut, 0);
    }

    public Shuffle getSHUFFLE() {
      return SHUFFLE;
    }

    public void destroy() {
      if (sslFactory != null) {
        sslFactory.destroy();
      }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      if (sslFactory != null) {
        pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
      }
      pipeline.addLast("decoder", new HttpRequestDecoder());
      pipeline.addLast("aggregator", new HttpChunkAggregator(1 << 16));
      pipeline.addLast("encoder", new HttpResponseEncoder());
      pipeline.addLast("chunking", new ChunkedWriteHandler());
      pipeline.addLast("shuffle", SHUFFLE);
      pipeline.addLast("idle", idleStateHandler);
      pipeline.addLast(TIMEOUT_HANDLER, new TimeoutHandler());
      return pipeline;
      // TODO factor security manager into pipeline
      // TODO factor out encode/decode to permit binary shuffle
      // TODO factor out decode of index to permit alt. models
    }

  }

  class Shuffle extends SimpleChannelUpstreamHandler {
    private static final int MAX_WEIGHT = 10 * 1024 * 1024;
    private static final int EXPIRE_AFTER_ACCESS_MINUTES = 5;
    private static final int ALLOWED_CONCURRENCY = 16;
    private final Configuration conf;
    private final IndexCache indexCache;
    private int port;
    private final LoadingCache<AttemptPathIdentifier, AttemptPathInfo> pathCache =
      CacheBuilder.newBuilder().expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES,
      TimeUnit.MINUTES).softValues().concurrencyLevel(ALLOWED_CONCURRENCY).
      removalListener(
          new RemovalListener<AttemptPathIdentifier, AttemptPathInfo>() {
            @Override
            public void onRemoval(RemovalNotification<AttemptPathIdentifier,
                AttemptPathInfo> notification) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("PathCache Eviction: " + notification.getKey() +
                    ", Reason=" + notification.getCause());
              }
            }
          }
      ).maximumWeight(MAX_WEIGHT).weigher(
          new Weigher<AttemptPathIdentifier, AttemptPathInfo>() {
            @Override
            public int weigh(AttemptPathIdentifier key,
                AttemptPathInfo value) {
              return key.jobId.length() + key.user.length() +
                  key.attemptId.length()+
                  value.indexPath.toString().length() +
                  value.dataPath.toString().length();
            }
          }
      ).build(new CacheLoader<AttemptPathIdentifier, AttemptPathInfo>() {
        @Override
        public AttemptPathInfo load(AttemptPathIdentifier key) throws
            Exception {
          String base = getBaseLocation(key.jobId, key.user);
          String attemptBase = base + key.attemptId;
          Path indexFileName = getAuxiliaryLocalPathHandler()
              .getLocalPathForRead(attemptBase + "/" + INDEX_FILE_NAME);
          Path mapOutputFileName = getAuxiliaryLocalPathHandler()
              .getLocalPathForRead(attemptBase + "/" + DATA_FILE_NAME);

          if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded : " + key + " via loader");
          }
          return new AttemptPathInfo(indexFileName, mapOutputFileName);
        }
      });

    public Shuffle(Configuration conf) {
      this.conf = conf;
      indexCache = new IndexCache(new JobConf(conf));
      this.port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    }

    public void setPort(int port) {
      this.port = port;
    }

    private List<String> splitMaps(List<String> mapq) {
      if (null == mapq) {
        return null;
      }
      final List<String> ret = new ArrayList<String>();
      for (String s : mapq) {
        Collections.addAll(ret, s.split(","));
      }
      return ret;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent evt) 
        throws Exception {
      super.channelOpen(ctx, evt);

      if ((maxShuffleConnections > 0) && (accepted.size() >= maxShuffleConnections)) {
        LOG.info(String.format("Current number of shuffle connections (%d) is " + 
            "greater than or equal to the max allowed shuffle connections (%d)", 
            accepted.size(), maxShuffleConnections));

        Map<String, String> headers = new HashMap<String, String>(1);
        // notify fetchers to backoff for a while before closing the connection
        // if the shuffle connection limit is hit. Fetchers are expected to
        // handle this notification gracefully, that is, not treating this as a
        // fetch failure.
        headers.put(RETRY_AFTER_HEADER, String.valueOf(FETCH_RETRY_DELAY));
        sendError(ctx, "", TOO_MANY_REQ_STATUS, headers);
        return;
      }
      accepted.add(evt.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent evt)
        throws Exception {
      HttpRequest request = (HttpRequest) evt.getMessage();
      if (request.getMethod() != GET) {
          sendError(ctx, METHOD_NOT_ALLOWED);
          return;
      }
      // Check whether the shuffle version is compatible
      if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(
          request.headers() != null ?
              request.headers().get(ShuffleHeader.HTTP_HEADER_NAME) : null)
          || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(
              request.headers() != null ?
                  request.headers()
                      .get(ShuffleHeader.HTTP_HEADER_VERSION) : null)) {
        sendError(ctx, "Incompatible shuffle request version", BAD_REQUEST);
      }
      final Map<String,List<String>> q =
        new QueryStringDecoder(request.getUri()).getParameters();
      final List<String> keepAliveList = q.get("keepAlive");
      boolean keepAliveParam = false;
      if (keepAliveList != null && keepAliveList.size() == 1) {
        keepAliveParam = Boolean.valueOf(keepAliveList.get(0));
        if (LOG.isDebugEnabled()) {
          LOG.debug("KeepAliveParam : " + keepAliveList
            + " : " + keepAliveParam);
        }
      }
      final List<String> mapIds = splitMaps(q.get("map"));
      final List<String> reduceQ = q.get("reduce");
      final List<String> jobQ = q.get("job");
      if (LOG.isDebugEnabled()) {
        LOG.debug("RECV: " + request.getUri() +
            "\n  mapId: " + mapIds +
            "\n  reduceId: " + reduceQ +
            "\n  jobId: " + jobQ +
            "\n  keepAlive: " + keepAliveParam);
      }

      if (mapIds == null || reduceQ == null || jobQ == null) {
        sendError(ctx, "Required param job, map and reduce", BAD_REQUEST);
        return;
      }
      if (reduceQ.size() != 1 || jobQ.size() != 1) {
        sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
        return;
      }

      int reduceId;
      String jobId;
      try {
        reduceId = Integer.parseInt(reduceQ.get(0));
        jobId = jobQ.get(0);
      } catch (NumberFormatException e) {
        sendError(ctx, "Bad reduce parameter", BAD_REQUEST);
        return;
      } catch (IllegalArgumentException e) {
        sendError(ctx, "Bad job parameter", BAD_REQUEST);
        return;
      }
      final String reqUri = request.getUri();
      if (null == reqUri) {
        // TODO? add upstream?
        sendError(ctx, FORBIDDEN);
        return;
      }
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      try {
        verifyRequest(jobId, ctx, request, response,
            new URL("http", "", this.port, reqUri));
      } catch (IOException e) {
        LOG.warn("Shuffle failure ", e);
        sendError(ctx, e.getMessage(), UNAUTHORIZED);
        return;
      }

      Map<String, MapOutputInfo> mapOutputInfoMap =
          new HashMap<String, MapOutputInfo>();
      Channel ch = evt.getChannel();
      ChannelPipeline pipeline = ch.getPipeline();
      TimeoutHandler timeoutHandler =
          (TimeoutHandler)pipeline.get(TIMEOUT_HANDLER);
      timeoutHandler.setEnabledTimeout(false);
      String user = userRsrc.get(jobId);

      try {
        populateHeaders(mapIds, jobId, user, reduceId, request,
          response, keepAliveParam, mapOutputInfoMap);
      } catch(IOException e) {
        ch.write(response);
        LOG.error("Shuffle error in populating headers :", e);
        String errorMessage = getErrorMessage(e);
        sendError(ctx,errorMessage , INTERNAL_SERVER_ERROR);
        return;
      }
      ch.write(response);
      //Initialize one ReduceContext object per messageReceived call
      boolean keepAlive = keepAliveParam || connectionKeepAliveEnabled;
      ReduceContext reduceContext = new ReduceContext(mapIds, reduceId, ctx,
          user, mapOutputInfoMap, jobId, keepAlive);
      for (int i = 0; i < Math.min(maxSessionOpenFiles, mapIds.size()); i++) {
        ChannelFuture nextMap = sendMap(reduceContext);
        if(nextMap == null) {
          return;
        }
      }
    }

    /**
     * Calls sendMapOutput for the mapId pointed by ReduceContext.mapsToSend
     * and increments it. This method is first called by messageReceived()
     * maxSessionOpenFiles times and then on the completion of every
     * sendMapOutput operation. This limits the number of open files on a node,
     * which can get really large(exhausting file descriptors on the NM) if all
     * sendMapOutputs are called in one go, as was done previous to this change.
     * @param reduceContext used to call sendMapOutput with correct params.
     * @return the ChannelFuture of the sendMapOutput, can be null.
     */
    public ChannelFuture sendMap(ReduceContext reduceContext)
        throws Exception {

      ChannelFuture nextMap = null;
      if (reduceContext.getMapsToSend().get() <
          reduceContext.getMapIds().size()) {
        int nextIndex = reduceContext.getMapsToSend().getAndIncrement();
        String mapId = reduceContext.getMapIds().get(nextIndex);

        try {
          MapOutputInfo info = reduceContext.getInfoMap().get(mapId);
          if (info == null) {
            info = getMapOutputInfo(mapId, reduceContext.getReduceId(),
                reduceContext.getJobId(), reduceContext.getUser());
          }
          nextMap = sendMapOutput(
              reduceContext.getCtx(),
              reduceContext.getCtx().getChannel(),
              reduceContext.getUser(), mapId,
              reduceContext.getReduceId(), info);
          if (null == nextMap) {
            sendError(reduceContext.getCtx(), NOT_FOUND);
            return null;
          }
          nextMap.addListener(new ReduceMapFileCount(reduceContext));
        } catch (IOException e) {
          if (e instanceof DiskChecker.DiskErrorException) {
            LOG.error("Shuffle error :" + e);
          } else {
            LOG.error("Shuffle error :", e);
          }
          String errorMessage = getErrorMessage(e);
          sendError(reduceContext.getCtx(), errorMessage,
              INTERNAL_SERVER_ERROR);
          return null;
        }
      }
      return nextMap;
    }

    private String getErrorMessage(Throwable t) {
      StringBuffer sb = new StringBuffer(t.getMessage());
      while (t.getCause() != null) {
        sb.append(t.getCause().getMessage());
        t = t.getCause();
      }
      return sb.toString();
    }

    private String getBaseLocation(String jobId, String user) {
      final JobID jobID = JobID.forName(jobId);
      final ApplicationId appID =
          ApplicationId.newInstance(Long.parseLong(jobID.getJtIdentifier()),
            jobID.getId());
      final String baseStr =
          ContainerLocalizer.USERCACHE + "/" + user + "/"
              + ContainerLocalizer.APPCACHE + "/"
              + appID.toString() + "/output" + "/";
      return baseStr;
    }

    protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
        String jobId, String user) throws IOException {
      AttemptPathInfo pathInfo;
      try {
        AttemptPathIdentifier identifier = new AttemptPathIdentifier(
            jobId, user, mapId);
        pathInfo = pathCache.get(identifier);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Retrieved pathInfo for " + identifier +
              " check for corresponding loaded messages to determine whether" +
              " it was loaded or cached");
        }
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else {
          throw new RuntimeException(e.getCause());
        }
      }

      IndexRecord info =
        indexCache.getIndexInformation(mapId, reduce, pathInfo.indexPath, user);

      if (LOG.isDebugEnabled()) {
        LOG.debug("getMapOutputInfo: jobId=" + jobId + ", mapId=" + mapId +
            ",dataFile=" + pathInfo.dataPath + ", indexFile=" +
            pathInfo.indexPath);
      }

      MapOutputInfo outputInfo = new MapOutputInfo(pathInfo.dataPath, info);
      return outputInfo;
    }

    protected void populateHeaders(List<String> mapIds, String jobId,
        String user, int reduce, HttpRequest request, HttpResponse response,
        boolean keepAliveParam, Map<String, MapOutputInfo> mapOutputInfoMap)
        throws IOException {

      long contentLength = 0;
      for (String mapId : mapIds) {
        MapOutputInfo outputInfo = getMapOutputInfo(mapId, reduce, jobId, user);
        if (mapOutputInfoMap.size() < mapOutputMetaInfoCacheSize) {
          mapOutputInfoMap.put(mapId, outputInfo);
        }

        ShuffleHeader header =
            new ShuffleHeader(mapId, outputInfo.indexRecord.partLength,
            outputInfo.indexRecord.rawLength, reduce);
        DataOutputBuffer dob = new DataOutputBuffer();
        header.write(dob);

        contentLength += outputInfo.indexRecord.partLength;
        contentLength += dob.getLength();
      }

      // Now set the response headers.
      setResponseHeaders(response, keepAliveParam, contentLength);

      // this audit log is disabled by default,
      // to turn it on please enable this audit log
      // on log4j.properties by uncommenting the setting
      if (AUDITLOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder("shuffle for ");
        sb.append(jobId).append(" reducer ").append(reduce);
        sb.append(" length ").append(contentLength);
        sb.append(" mappers: ").append(mapIds);
        AUDITLOG.debug(sb.toString());
      }
    }

    protected void setResponseHeaders(HttpResponse response,
        boolean keepAliveParam, long contentLength) {
      if (!connectionKeepAliveEnabled && !keepAliveParam) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting connection close header...");
        }
        response.headers().set(HttpHeader.CONNECTION.asString(),
            CONNECTION_CLOSE);
      } else {
        response.headers().set(HttpHeader.CONTENT_LENGTH.asString(),
          String.valueOf(contentLength));
        response.headers().set(HttpHeader.CONNECTION.asString(),
            HttpHeader.KEEP_ALIVE.asString());
        response.headers().set(HttpHeader.KEEP_ALIVE.asString(),
            "timeout=" + connectionKeepAliveTimeOut);
        LOG.info("Content Length in shuffle : " + contentLength);
      }
    }

    class MapOutputInfo {
      final Path mapOutputFileName;
      final IndexRecord indexRecord;

      MapOutputInfo(Path mapOutputFileName, IndexRecord indexRecord) {
        this.mapOutputFileName = mapOutputFileName;
        this.indexRecord = indexRecord;
      }
    }

    protected void verifyRequest(String appid, ChannelHandlerContext ctx,
        HttpRequest request, HttpResponse response, URL requestUri)
        throws IOException {
      SecretKey tokenSecret = secretManager.retrieveTokenSecret(appid);
      if (null == tokenSecret) {
        LOG.info("Request for unknown token " + appid);
        throw new IOException("could not find jobid");
      }
      // string to encrypt
      String enc_str = SecureShuffleUtils.buildMsgFrom(requestUri);
      // hash from the fetcher
      String urlHashStr =
          request.headers().get(SecureShuffleUtils.HTTP_HEADER_URL_HASH);
      if (urlHashStr == null) {
        LOG.info("Missing header hash for " + appid);
        throw new IOException("fetcher cannot be authenticated");
      }
      if (LOG.isDebugEnabled()) {
        int len = urlHashStr.length();
        LOG.debug("verifying request. enc_str=" + enc_str + "; hash=..." +
            urlHashStr.substring(len-len/2, len-1));
      }
      // verify - throws exception
      SecureShuffleUtils.verifyReply(urlHashStr, enc_str, tokenSecret);
      // verification passed - encode the reply
      String reply =
        SecureShuffleUtils.generateHash(urlHashStr.getBytes(Charsets.UTF_8), 
            tokenSecret);
      response.headers().set(
          SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH, reply);
      // Put shuffle version into http header
      response.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      if (LOG.isDebugEnabled()) {
        int len = reply.length();
        LOG.debug("Fetcher request verfied. enc_str=" + enc_str + ";reply=" +
            reply.substring(len-len/2, len-1));
      }
    }

    protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch,
        String user, String mapId, int reduce, MapOutputInfo mapOutputInfo)
        throws IOException {
      final IndexRecord info = mapOutputInfo.indexRecord;
      final ShuffleHeader header =
        new ShuffleHeader(mapId, info.partLength, info.rawLength, reduce);
      final DataOutputBuffer dob = new DataOutputBuffer();
      header.write(dob);
      ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
      final File spillfile =
          new File(mapOutputInfo.mapOutputFileName.toString());
      RandomAccessFile spill;
      try {
        spill = SecureIOUtils.openForRandomRead(spillfile, "r", user, null);
      } catch (FileNotFoundException e) {
        LOG.info(spillfile + " not found");
        return null;
      }
      ChannelFuture writeFuture;
      if (ch.getPipeline().get(SslHandler.class) == null) {
        final FadvisedFileRegion partition = new FadvisedFileRegion(spill,
            info.startOffset, info.partLength, manageOsCache, readaheadLength,
            readaheadPool, spillfile.getAbsolutePath(), 
            shuffleBufferSize, shuffleTransferToAllowed);
        writeFuture = ch.write(partition);
        writeFuture.addListener(new ChannelFutureListener() {
            // TODO error handling; distinguish IO/connection failures,
            //      attribute to appropriate spill output
          @Override
          public void operationComplete(ChannelFuture future) {
            if (future.isSuccess()) {
              partition.transferSuccessful();
            }
            partition.releaseExternalResources();
          }
        });
      } else {
        // HTTPS cannot be done with zero copy.
        final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
            info.startOffset, info.partLength, sslFileBufferSize,
            manageOsCache, readaheadLength, readaheadPool,
            spillfile.getAbsolutePath());
        writeFuture = ch.write(chunk);
      }
      metrics.shuffleConnections.incr();
      metrics.shuffleOutputBytes.incr(info.partLength); // optimistic
      return writeFuture;
    }

    protected void sendError(ChannelHandlerContext ctx,
        HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    protected void sendError(ChannelHandlerContext ctx, String message,
        HttpResponseStatus status) {
      sendError(ctx, message, status, Collections.<String, String>emptyMap());
    }

    protected void sendError(ChannelHandlerContext ctx, String msg,
        HttpResponseStatus status, Map<String, String> headers) {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
      response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
      // Put shuffle version into http header
      response.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      for (Map.Entry<String, String> header : headers.entrySet()) {
        response.headers().set(header.getKey(), header.getValue());
      }
      response.setContent(
          ChannelBuffers.copiedBuffer(msg, CharsetUtil.UTF_8));

      // Close the connection as soon as the error message is sent.
      ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      Channel ch = e.getChannel();
      Throwable cause = e.getCause();
      if (cause instanceof TooLongFrameException) {
        sendError(ctx, BAD_REQUEST);
        return;
      } else if (cause instanceof IOException) {
        if (cause instanceof ClosedChannelException) {
          LOG.debug("Ignoring closed channel error", cause);
          return;
        }
        String message = String.valueOf(cause.getMessage());
        if (IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
          LOG.debug("Ignoring client socket close", cause);
          return;
        }
      }

      LOG.error("Shuffle error: ", cause);
      if (ch.isConnected()) {
        LOG.error("Shuffle error " + e);
        sendError(ctx, INTERNAL_SERVER_ERROR);
      }
    }
  }
  
  static class AttemptPathInfo {
    // TODO Change this over to just store local dir indices, instead of the
    // entire path. Far more efficient.
    private final Path indexPath;
    private final Path dataPath;

    public AttemptPathInfo(Path indexPath, Path dataPath) {
      this.indexPath = indexPath;
      this.dataPath = dataPath;
    }
  }

  static class AttemptPathIdentifier {
    private final String jobId;
    private final String user;
    private final String attemptId;

    public AttemptPathIdentifier(String jobId, String user, String attemptId) {
      this.jobId = jobId;
      this.user = user;
      this.attemptId = attemptId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AttemptPathIdentifier that = (AttemptPathIdentifier) o;

      if (!attemptId.equals(that.attemptId)) {
        return false;
      }
      if (!jobId.equals(that.jobId)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = jobId.hashCode();
      result = 31 * result + attemptId.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "AttemptPathIdentifier{" +
          "attemptId='" + attemptId + '\'' +
          ", jobId='" + jobId + '\'' +
          '}';
    }
  }
}
