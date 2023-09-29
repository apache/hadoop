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

package org.apache.hadoop.mapred;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

import javax.annotation.Nonnull;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.proto.ShuffleHandlerRecoveryProtos.JobShuffleInfoProto;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
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
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

public class ShuffleHandler extends AuxiliaryService {

  public static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(ShuffleHandler.class);
  public static final org.slf4j.Logger AUDITLOG =
      LoggerFactory.getLogger(ShuffleHandler.class.getName()+".audit");
  public static final String SHUFFLE_MANAGE_OS_CACHE = "mapreduce.shuffle.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_READAHEAD_BYTES = "mapreduce.shuffle.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;

  public static final String MAX_WEIGHT =
      "mapreduce.shuffle.pathcache.max-weight";
  public static final int DEFAULT_MAX_WEIGHT = 10 * 1024 * 1024;

  public static final String EXPIRE_AFTER_ACCESS_MINUTES =
      "mapreduce.shuffle.pathcache.expire-after-access-minutes";
  public static final int DEFAULT_EXPIRE_AFTER_ACCESS_MINUTES = 5;

  public static final String CONCURRENCY_LEVEL =
      "mapreduce.shuffle.pathcache.concurrency-level";
  public static final int DEFAULT_CONCURRENCY_LEVEL = 16;
  
  // pattern to identify errors related to the client closing the socket early
  // idea borrowed from Netty SslHandler
  public static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
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
  // This should be kept in sync with Fetcher.FETCH_RETRY_DELAY_DEFAULT
  public static final long FETCH_RETRY_DELAY = 1000L;
  public static final String RETRY_AFTER_HEADER = "Retry-After";

  private int port;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final ChannelGroup allChannels =
      new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private SSLFactory sslFactory;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected JobTokenSecretManager secretManager;
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected Map<String, String> userRsrc;

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
  static final String TIMEOUT_HANDLER = "timeout";

  /* the maximum number of files a single GET request can
   open simultaneously during shuffle
   */
  public static final String SHUFFLE_MAX_SESSION_OPEN_FILES =
      "mapreduce.shuffle.max.session-open-files";
  public static final int DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES = 3;


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

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final MetricsSystem ms;
  @SuppressWarnings("checkstyle:VisibilityModifier")
  final ShuffleMetrics metrics;

  ShuffleHandler(MetricsSystem ms) {
    super(MAPREDUCE_SHUFFLE_SERVICEID);
    this.ms = ms;
    metrics = ms.register(new ShuffleMetrics());
  }

  public ShuffleHandler() {
    this(DefaultMetricsSystem.instance());
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * @param port the port to be sent to the ApplciationMaster
   * @return the serialized form of the port.
   * @throws IOException on failure
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer portDob = new DataOutputBuffer();
    portDob.writeInt(port);
    return ByteBuffer.wrap(portDob.getData(), 0, portDob.getLength());
  }

  /**
   * A helper function to deserialize the metadata returned by ShuffleHandler.
   * @param meta the metadata returned by the ShuffleHandler
   * @return the port the Shuffle Handler is listening on to serve shuffle data.
   * @throws IOException on failure
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
   * @throws IOException on failure
   */
  public static ByteBuffer serializeServiceData(Token<JobTokenIdentifier> jobToken)
      throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer jobTokenDob = new DataOutputBuffer();
    jobToken.write(jobTokenDob);
    return ByteBuffer.wrap(jobTokenDob.getData(), 0, jobTokenDob.getLength());
  }

  public static Token<JobTokenIdentifier> deserializeServiceData(ByteBuffer secret)
      throws IOException {
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
    int maxShuffleThreads = conf.getInt(MAX_SHUFFLE_THREADS,
                                        DEFAULT_MAX_SHUFFLE_THREADS);
    // Since Netty 4.x, the value of 0 threads would default to:
    // io.netty.channel.MultithreadEventLoopGroup.DEFAULT_EVENT_LOOP_THREADS
    // by simply passing 0 value to NioEventLoopGroup constructor below.
    // However, this logic to determinte thread count
    // was in place so we can keep it for now.
    if (maxShuffleThreads == 0) {
      maxShuffleThreads = 2 * Runtime.getRuntime().availableProcessors();
    }

    ThreadFactory bossFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Boss #%d")
        .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Worker #%d")
        .build();
    
    bossGroup = new NioEventLoopGroup(1, bossFactory);
    workerGroup = new NioEventLoopGroup(maxShuffleThreads, workerFactory);
    super.serviceInit(new Configuration(conf));
  }

  protected ShuffleChannelHandlerContext createHandlerContext() {
    Configuration conf = getConfig();

    final LoadingCache<AttemptPathIdentifier, AttemptPathInfo> pathCache =
        CacheBuilder.newBuilder().expireAfterAccess(
                conf.getInt(EXPIRE_AFTER_ACCESS_MINUTES, DEFAULT_EXPIRE_AFTER_ACCESS_MINUTES),
                TimeUnit.MINUTES).softValues().concurrencyLevel(conf.getInt(CONCURRENCY_LEVEL,
                DEFAULT_CONCURRENCY_LEVEL)).
            removalListener(
                (RemovalListener<AttemptPathIdentifier, AttemptPathInfo>) notification -> {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("PathCache Eviction: " + notification.getKey() +
                        ", Reason=" + notification.getCause());
                  }
                }
            ).maximumWeight(conf.getInt(MAX_WEIGHT, DEFAULT_MAX_WEIGHT)).weigher(
                (key, value) -> key.jobId.length() + key.user.length() +
                    key.attemptId.length()+
                    value.indexPath.toString().length() +
                    value.dataPath.toString().length()
            ).build(new CacheLoader<AttemptPathIdentifier, AttemptPathInfo>() {
              @Override
              public AttemptPathInfo load(@Nonnull AttemptPathIdentifier key) throws
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

    return new ShuffleChannelHandlerContext(conf,
        userRsrc,
        secretManager,
        pathCache,
        new IndexCache(new JobConf(conf)),
        metrics,
        allChannels
    );
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    userRsrc = new ConcurrentHashMap<>();
    secretManager = new JobTokenSecretManager();
    recoverState(conf);

    if (conf.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
        MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT)) {
      LOG.info("Encrypted shuffle is enabled.");
      sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
      sslFactory.init();
    }

    ShuffleChannelHandlerContext handlerContext = createHandlerContext();
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG,
            conf.getInt(SHUFFLE_LISTEN_QUEUE_SIZE,
                DEFAULT_SHUFFLE_LISTEN_QUEUE_SIZE))
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new ShuffleChannelInitializer(
            handlerContext,
            sslFactory)
        );
    port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    Channel ch = bootstrap.bind(new InetSocketAddress(port)).sync().channel();
    port = ((InetSocketAddress)ch.localAddress()).getPort();
    allChannels.add(ch);
    conf.set(SHUFFLE_PORT_CONFIG_KEY, Integer.toString(port));
    handlerContext.setPort(port);
    LOG.info(getName() + " listening on port " + port);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    allChannels.close().awaitUninterruptibly(10, TimeUnit.SECONDS);

    if (sslFactory != null) {
      sslFactory.destroy();
    }

    if (stateDb != null) {
      stateDb.close();
    }
    ms.unregisterSource(ShuffleMetrics.class.getSimpleName());

    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
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
    Token<JobTokenIdentifier> jobToken = new Token<>(
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

  static class TimeoutHandler extends IdleStateHandler {
    private final int connectionKeepAliveTimeOut;
    private boolean enabledTimeout;

    TimeoutHandler(int connectionKeepAliveTimeOut) {
      //disable reader timeout
      //set writer timeout to configured timeout value
      //disable all idle timeout
      super(0, connectionKeepAliveTimeOut, 0, TimeUnit.SECONDS);
      this.connectionKeepAliveTimeOut = connectionKeepAliveTimeOut;
    }

    void setEnabledTimeout(boolean enabledTimeout) {
      this.enabledTimeout = enabledTimeout;
    }

    @Override
    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) {
      if (e.state() == IdleState.WRITER_IDLE && enabledTimeout) {
        LOG.debug("Closing channel as writer was idle for {} seconds", connectionKeepAliveTimeOut);
        ctx.channel().close();
      }
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  static class AttemptPathInfo {
    // TODO Change this over to just store local dir indices, instead of the
    // entire path. Far more efficient.
    public final Path indexPath;
    public final Path dataPath;

    AttemptPathInfo(Path indexPath, Path dataPath) {
      this.indexPath = indexPath;
      this.dataPath = dataPath;
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  static class AttemptPathIdentifier {
    public final String jobId;
    public final String user;
    public final String attemptId;

    AttemptPathIdentifier(String jobId, String user, String attemptId) {
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

  private static String getBaseLocation(String jobId, String user) {
    final JobID jobID = JobID.forName(jobId);
    final ApplicationId appID =
        ApplicationId.newInstance(Long.parseLong(jobID.getJtIdentifier()),
            jobID.getId());
    return ContainerLocalizer.USERCACHE + "/" + user + "/"
        + ContainerLocalizer.APPCACHE + "/"
        + appID + "/output" + "/";
  }
}
