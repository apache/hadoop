/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.web.netty;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import java.io.Closeable;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.util.Enumeration;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;

import io.netty.bootstrap.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .HDDS_REST_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .HDDS_REST_HTTP_ADDRESS_KEY;

/**
 * Netty based web server for Hdds rest api server.
 * <p>
 * Based on the Datanode http serer.
 */
public class ObjectStoreRestHttpServer implements Closeable {
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ServerSocketChannel externalHttpChannel;
  private final ServerBootstrap httpServer;
  private final Configuration conf;
  private final Configuration confForCreate;
  private InetSocketAddress httpAddress;
  static final Log LOG = LogFactory.getLog(ObjectStoreRestHttpServer.class);
  private final ObjectStoreHandler objectStoreHandler;

  public ObjectStoreRestHttpServer(final Configuration conf,
      final ServerSocketChannel externalHttpChannel,
      ObjectStoreHandler objectStoreHandler) throws IOException {
    this.conf = conf;

    this.confForCreate = new Configuration(conf);
    this.objectStoreHandler = objectStoreHandler;
    confForCreate.set(FsPermission.UMASK_LABEL, "000");

    this.bossGroup = new NioEventLoopGroup();
    this.workerGroup = new NioEventLoopGroup();
    this.externalHttpChannel = externalHttpChannel;

    this.httpServer = new ServerBootstrap();
    this.httpServer.group(bossGroup, workerGroup);
    this.httpServer.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpRequestDecoder(), new HttpResponseEncoder());
        // Later we have to support cross-site request forgery (CSRF) Filter
        p.addLast(new ChunkedWriteHandler(), new ObjectStoreURLDispatcher(
            objectStoreHandler.getObjectStoreJerseyContainer()));
      }
    });

    this.httpServer.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
        conf.getInt(ScmConfigKeys.HDDS_REST_NETTY_HIGH_WATERMARK,
            ScmConfigKeys.HDDS_REST_NETTY_HIGH_WATERMARK_DEFAULT));
    this.httpServer.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
        conf.getInt(ScmConfigKeys.HDDS_REST_NETTY_LOW_WATERMARK,
            ScmConfigKeys.HDDS_REST_NETTY_LOW_WATERMARK_DEFAULT));

    if (externalHttpChannel == null) {
      httpServer.channel(NioServerSocketChannel.class);
    } else {
      httpServer.channelFactory(
          (ChannelFactory<NioServerSocketChannel>) () -> new
              NioServerSocketChannel(
              externalHttpChannel) {
            // The channel has been bounded externally via JSVC,
            // thus bind() becomes a no-op.
            @Override
            protected void doBind(SocketAddress localAddress) throws Exception {
            }
          });
    }

  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public void start() throws IOException {
    if (httpServer != null) {

      InetSocketAddress infoAddr = NetUtils.createSocketAddr(
          conf.getTrimmed(HDDS_REST_HTTP_ADDRESS_KEY,
              HDDS_REST_HTTP_ADDRESS_DEFAULT));

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
      LOG.info("Listening HDDS REST traffic on " + httpAddress);
    }

  }

  @Override
  public void close() throws IOException {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    if (externalHttpChannel != null) {
      externalHttpChannel.close();
    }
  }

  /**
   * A minimal {@link FilterConfig} implementation backed by a {@link Map}.
   */
  private static final class MapBasedFilterConfig implements FilterConfig {

    private final String filterName;
    private final Map<String, String> parameters;

    /**
     * Creates a new MapBasedFilterConfig.
     *
     * @param filterName filter name
     * @param parameters mapping of filter initialization parameters
     */
    MapBasedFilterConfig(String filterName,
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

    /**
     * Creates an exception indicating that an interface method is not
     * implemented.  These should never be seen in practice, because it is only
     * used for methods that are not called by {@link RestCsrfPreventionFilter}.
     *
     * @return exception indicating method not implemented
     */
    private UnsupportedOperationException notImplemented() {
      return new UnsupportedOperationException(
          this.getClass().getSimpleName() + " does not implement this method.");
    }
  }
}

