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
package org.apache.hadoop.hdfs.server.datanode.web;

import io.netty.bootstrap.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.web.webhdfs.DataNodeUGIProvider;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_INTERNAL_PROXY_PORT;

/**
 * Data node HTTP Server Class.
 */
public class DatanodeHttpServer implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(DatanodeHttpServer.class);
  private static final ConcurrentHashMap<Class<?>, Object> HANDLER_STATE
      = new ConcurrentHashMap<Class<?>, Object>() {};
  // HttpServer threads are only used for the web UI and basic servlets, so
  // set them to the minimum possible
  private static final int HTTP_SELECTOR_THREADS = 1;
  private static final int HTTP_ACCEPTOR_THREADS = 1;
  // Jetty 9.4.x: Adding one more thread to HTTP_MAX_THREADS.
  private static final int HTTP_MAX_THREADS =
      HTTP_SELECTOR_THREADS + HTTP_ACCEPTOR_THREADS + 2;
  private final HttpServer2 infoServer;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ServerSocketChannel externalHttpChannel;
  private final ServerBootstrap httpServer;
  private final SSLFactory sslFactory;
  private final ServerBootstrap httpsServer;
  private final Configuration conf;
  private final Configuration confForCreate;
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;

  public DatanodeHttpServer(final Configuration conf,
        final DataNode datanode,
        final ServerSocketChannel externalHttpChannel)
        throws IOException {
    this.conf = conf;

    Configuration confForInfoServer = new Configuration(conf);
    confForInfoServer.setInt(HttpServer2.HTTP_MAX_THREADS_KEY,
        HTTP_MAX_THREADS);
    confForInfoServer.setInt(HttpServer2.HTTP_SELECTOR_COUNT_KEY,
        HTTP_SELECTOR_THREADS);
    confForInfoServer.setInt(HttpServer2.HTTP_ACCEPTOR_COUNT_KEY,
        HTTP_ACCEPTOR_THREADS);
    int proxyPort =
        confForInfoServer.getInt(DFS_DATANODE_HTTP_INTERNAL_PROXY_PORT, 0);
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("datanode")
        .setConf(confForInfoServer)
        .setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
        .hostName(getHostnameForSpnegoPrincipal(confForInfoServer))
        .addEndpoint(URI.create("http://localhost:" + proxyPort))
        .setFindPort(true);

    final boolean xFrameEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    this.infoServer = builder.build();

    this.infoServer.setAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE, conf);
    this.infoServer.setAttribute("datanode", datanode);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.addServlet(null, "/blockScannerReport",
        BlockScanner.Servlet.class);
    DataNodeUGIProvider.init(conf);
    this.infoServer.start();
    final InetSocketAddress jettyAddr = infoServer.getConnectorAddress(0);

    this.confForCreate = new Configuration(conf);
    confForCreate.set(FsPermission.UMASK_LABEL, "000");

    this.bossGroup = new NioEventLoopGroup();
    this.workerGroup = new NioEventLoopGroup();
    this.externalHttpChannel = externalHttpChannel;
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    final ChannelHandler[] handlers = getFilterHandlers(conf);

    if (policy.isHttpEnabled()) {
      this.httpServer = new ServerBootstrap().group(bossGroup, workerGroup)
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast(new HttpRequestDecoder(),
                    new HttpResponseEncoder());
                if (handlers != null) {
                  for (ChannelHandler c : handlers) {
                    p.addLast(c);
                  }
                }
                p.addLast(
                    new ChunkedWriteHandler(),
                    new URLDispatcher(jettyAddr, conf, confForCreate));
              }
            });

      this.httpServer.childOption(
          ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
          conf.getInt(
              DFSConfigKeys.DFS_WEBHDFS_NETTY_HIGH_WATERMARK,
              DFSConfigKeys.DFS_WEBHDFS_NETTY_HIGH_WATERMARK_DEFAULT));
      this.httpServer.childOption(
          ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
          conf.getInt(
              DFSConfigKeys.DFS_WEBHDFS_NETTY_LOW_WATERMARK,
              DFSConfigKeys.DFS_WEBHDFS_NETTY_LOW_WATERMARK_DEFAULT));

      if (externalHttpChannel == null) {
        httpServer.channel(NioServerSocketChannel.class);
      } else {
        httpServer.channelFactory(new ChannelFactory<NioServerSocketChannel>() {
          @Override
          public NioServerSocketChannel newChannel() {
            return new NioServerSocketChannel(externalHttpChannel) {
              // The channel has been bounded externally via JSVC,
              // thus bind() becomes a no-op.
              @Override
              protected void doBind(SocketAddress localAddress)
                  throws Exception {
              }
            };
          }
        });
      }
    } else {
      this.httpServer = null;
    }

    if (policy.isHttpsEnabled()) {
      this.sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
      try {
        sslFactory.init();
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
      this.httpsServer = new ServerBootstrap().group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline p = ch.pipeline();
              p.addLast(
                  new SslHandler(sslFactory.createSSLEngine()),
                  new HttpRequestDecoder(),
                  new HttpResponseEncoder());
              if (handlers != null) {
                for (ChannelHandler c : handlers) {
                  p.addLast(c);
                }
              }
              p.addLast(
                  new ChunkedWriteHandler(),
                  new URLDispatcher(jettyAddr, conf, confForCreate));
            }
          });
    } else {
      this.httpsServer = null;
      this.sslFactory = null;
    }
  }

  private static String getHostnameForSpnegoPrincipal(Configuration conf) {
    String addr = conf.getTrimmed(DFS_DATANODE_HTTP_ADDRESS_KEY, null);
    if (addr == null) {
      addr = conf.getTrimmed(DFS_DATANODE_HTTPS_ADDRESS_KEY,
          DFS_DATANODE_HTTPS_ADDRESS_DEFAULT);
    }
    InetSocketAddress inetSocker = NetUtils.createSocketAddr(addr);
    return inetSocker.getHostString();
  }

  /* Get an array of ChannelHandlers specified in the conf
   * @param conf configuration to read and pass
   * @return array of ChannelHandlers ready to be used
   * @throws NoSuchMethodException if the handler does not implement a method
   *  initializeState(conf)
   * @throws InvocationTargetException if the handler's initalizeState method
   *  raises an exception
   */
  private ChannelHandler[] getFilterHandlers(Configuration configuration) {
    if (configuration == null) {
      return null;
    }
    // If the hdfs-site.xml has the proper configs for filter classes, use them.
    Class<?>[] classes =
        configuration.getClasses(
            DFSConfigKeys.DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS);

    // else use the hard coded class from the default configuration.
    if (classes == null) {
      classes =
          configuration.getClasses(
              DFSConfigKeys.DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS_DEFAULT);
    }

    // if we are not able to find any handlers, let us fail since running
    // with Csrf will is a security hole. Let us abort the startup.
    if(classes == null)  {
      return null;
    }

    ChannelHandler[] handlers = new ChannelHandler[classes.length];
    for (int i = 0; i < classes.length; i++) {
      LOG.debug("Loading filter handler {}", classes[i].getName());
      try {
        Method initializeState = classes[i].getDeclaredMethod("initializeState",
            Configuration.class);
        Constructor constructor =
            classes[i].getDeclaredConstructor(initializeState.getReturnType());
        handlers[i] = (ChannelHandler) constructor.newInstance(
            HANDLER_STATE.getOrDefault(classes[i],
            initializeState.invoke(null, configuration)));
      } catch (NoSuchMethodException | InvocationTargetException
          | IllegalAccessException | InstantiationException
          | IllegalArgumentException e) {
        LOG.error("Failed to initialize handler {}", classes[i].toString());
        throw new RuntimeException(e);
      }
    }
    return (handlers);
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  public void start() throws IOException {
    if (httpServer != null) {
      InetSocketAddress infoAddr = DataNode.getInfoAddr(conf);
      ChannelFuture f = httpServer.bind(infoAddr);
      try {
        f.syncUninterruptibly();
      } catch (Throwable e) {
        if (e instanceof BindException) {
          throw NetUtils.wrapException(null, 0, infoAddr.getHostName(),
              infoAddr.getPort(), (SocketException) e);
        } else {
          throw e;
        }
      }
      httpAddress = (InetSocketAddress) f.channel().localAddress();
      LOG.info("Listening HTTP traffic on " + httpAddress);
    }

    if (httpsServer != null) {
      InetSocketAddress secInfoSocAddr =
          NetUtils.createSocketAddr(conf.getTrimmed(
              DFS_DATANODE_HTTPS_ADDRESS_KEY,
              DFS_DATANODE_HTTPS_ADDRESS_DEFAULT));
      ChannelFuture f = httpsServer.bind(secInfoSocAddr);

      try {
        f.syncUninterruptibly();
      } catch (Throwable e) {
        if (e instanceof BindException) {
          throw NetUtils.wrapException(null, 0, secInfoSocAddr.getHostName(),
              secInfoSocAddr.getPort(), (SocketException) e);
        } else {
          throw e;
        }
      }
      httpsAddress = (InetSocketAddress) f.channel().localAddress();
      LOG.info("Listening HTTPS traffic on " + httpsAddress);
    }
  }

  @Override
  public void close() throws IOException {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    if (sslFactory != null) {
      sslFactory.destroy();
    }
    if (externalHttpChannel != null) {
      externalHttpChannel.close();
    }
    try {
      infoServer.stop();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Since the DataNode HTTP server is not implemented in terms of the
   * servlet API, it
   * takes some extra effort to obtain an instance of the filter.  This
   * method provides
   * a minimal {@link FilterConfig} implementation backed by a {@link Map}.
   * Call this from
   * your filter handler to initialize a servlet filter.
   */
  public static final class MapBasedFilterConfig implements FilterConfig {

    private final String filterName;
    private final Map<String, String> parameters;

    /*
     * Creates a new MapBasedFilterConfig.
     *
     * @param filterName filter name
     * @param parameters mapping of filter initialization parameters
     */
    public MapBasedFilterConfig(String filterName,
        Map<String, String> parameters) {
      this.filterName = filterName;
      this.parameters = parameters;
    }

    @Override
    public String getFilterName() {
      return this.filterName;
    }

    @Override
    public String getInitParameter(String name) {
      return this.parameters.get(name);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      throw this.notImplemented();
    }

    @Override
    public ServletContext getServletContext() {
      throw this.notImplemented();
    }

    /*
     * Creates an exception indicating that an interface method is not
     * implemented. If you are building a handler it is possible you will
     * need to make this interface more extensive.
     *
     * @return exception indicating method not implemented
     */
    private UnsupportedOperationException notImplemented() {
      return new UnsupportedOperationException(this.getClass().getSimpleName()
          + " does not implement this method.");
    }
  }
}
