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

package org.apache.hadoop.ozone.web.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import static io.netty.util.CharsetUtil.UTF_8;

/**
 * Unit tests for Ozone client connection reuse with Apache HttpClient and Netty
 * based HttpClient.
 */
public class TestOzoneClient {
  private static Logger log = Logger.getLogger(TestOzoneClient.class);
  private static int testVolumeCount = 5;
  private static MiniOzoneCluster cluster = null;
  private static String endpoint = null;

  @BeforeClass
  public static void init() throws Exception {
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.ALL);
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    int port = cluster.getHddsDatanodes().get(0)
        .getDatanodeDetails()
        .getPort(DatanodeDetails.Port.Name.REST).getValue();
    endpoint = String.format("http://localhost:%d", port);
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 5000)
  public void testNewConnectionPerRequest()
      throws IOException, URISyntaxException {
    for (int i = 0; i < testVolumeCount; i++) {
      try (CloseableHttpClient httpClient =
               HttpClients.createDefault()) {
        createVolume(getRandomVolumeName(i), httpClient);
      }
    }
  }

  /**
   * Object handler should be able to serve multiple requests from
   * a single http client. This allows the client side to reuse
   * http connections in a connection pool instead of creating a new
   * connection per request which consumes resource heavily.
   *
   */
  @Test(timeout = 5000)
  public void testReuseWithApacheHttpClient()
      throws IOException, URISyntaxException {

    PoolingHttpClientConnectionManager cm =
        new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(200);
    cm.setDefaultMaxPerRoute(20);

    try (CloseableHttpClient httpClient =
             HttpClients.custom().setConnectionManager(cm).build()) {
      for (int i = 0; i < testVolumeCount; i++) {
        createVolume(getRandomVolumeName(i), httpClient);
      }
    }
  }

  @Test(timeout = 10000)
  public void testReuseWithNettyHttpClient()
      throws IOException, InterruptedException, URISyntaxException {
    URI uri = new URI(endpoint);
    String host = uri.getHost() == null? "127.0.0.1" : uri.getHost();
    int port = uri.getPort();

    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      Bootstrap b = new Bootstrap();
      b.group(workerGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.SO_REUSEADDR, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            /**
             * This method will be called once the {@link Channel} was
             * registered. After the method returns this instance
             * will be removed from the {@link ChannelPipeline}
             * of the {@link Channel}.
             *
             * @param ch the {@link Channel} which was registered.
             * @throws Exception is thrown if an error occurs.
             * In that case the {@link Channel} will be closed.
             */
            @Override
            public void initChannel(SocketChannel ch) {
              ChannelPipeline p = ch.pipeline();

              // Comment the following line if you don't want client http trace
              p.addLast("log", new LoggingHandler(LogLevel.INFO));
              p.addLast(new HttpClientCodec());
              p.addLast(new HttpContentDecompressor());
              p.addLast(new NettyHttpClientHandler());
            }
          });

      Channel ch = b.connect(host, port).sync().channel();
      for (int i = 0; i < testVolumeCount; i++) {
        String volumeName = getRandomVolumeName(i);
        try {
          sendNettyCreateVolumeRequest(ch, volumeName);
          Thread.sleep(1000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      Thread.sleep(1000);
      ch.close();
      // Wait for the server to close the connection.
      ch.closeFuture().sync();
    } catch (Exception ex) {
      log.error("Error received in client setup", ex);
    }finally {
      workerGroup.shutdownGracefully();
    }
  }

  class NettyHttpClientHandler extends
      SimpleChannelInboundHandler<HttpObject> {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
      if (msg instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) msg;
        log.info("STATUS: " + response.getStatus());
        log.info("VERSION: " + response.getProtocolVersion());
        Assert.assertEquals(HttpResponseStatus.CREATED.code(),
            response.getStatus().code());
      }
      if (msg instanceof HttpContent) {
        HttpContent content = (HttpContent) msg;
        log.info(content.content().toString(UTF_8));
        if (content instanceof LastHttpContent) {
          log.info("END OF CONTENT");
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.error("Exception upon channel read", cause);
      ctx.close();
    }
  }

  private String getRandomVolumeName(int index) {
    UUID id = UUID.randomUUID();
    return "test-volume-" + index + "-" + id;

  }

  // Prepare the HTTP request and send it over the netty channel.
  private void sendNettyCreateVolumeRequest(Channel channel, String volumeName)
      throws URISyntaxException, IOException {
    URIBuilder builder = new URIBuilder(endpoint);
    builder.setPath("/" + volumeName);
    URI uri = builder.build();

    String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
    FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath());

    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    request.headers().set(HttpHeaders.HOST, host);
    request.headers().add(HttpHeaders.CONTENT_TYPE, "application/json");
    request.headers().set(Header.OZONE_VERSION_HEADER,
        Header.OZONE_V1_VERSION_HEADER);
    request.headers().set(HttpHeaders.DATE,
        format.format(new Date(Time.monotonicNow())));
    request.headers().set(Header.OZONE_USER,
        UserGroupInformation.getCurrentUser().getUserName());
    request.headers().set(HttpHeaders.AUTHORIZATION,
        Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " "
            + OzoneConsts.OZONE_SIMPLE_HDFS_USER);

    // Send the HTTP request via netty channel.
    channel.writeAndFlush(request);
  }

  // It is caller's responsibility to close the client.
  private void createVolume(String volumeName, CloseableHttpClient httpClient)
      throws IOException, URISyntaxException {
    HttpPost create1 =
        getCreateVolumeRequest(volumeName);
    HttpEntity entity = null;
    try {
      CloseableHttpResponse response1 =
          httpClient.execute(create1);
      Assert.assertEquals(HttpURLConnection.HTTP_CREATED,
          response1.getStatusLine().getStatusCode());
      entity = response1.getEntity();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      EntityUtils.consumeQuietly(entity);
    }
  }

  private HttpPost getCreateVolumeRequest(String volumeName)
      throws URISyntaxException, IOException {
    URIBuilder builder = new URIBuilder(endpoint);
    builder.setPath("/" + volumeName);
    HttpPost httpPost = new HttpPost(builder.build().toString());
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    httpPost.addHeader(Header.OZONE_VERSION_HEADER,
        Header.OZONE_V1_VERSION_HEADER);
    httpPost.addHeader(HttpHeaders.DATE,
        format.format(new Date(Time.monotonicNow())));
    httpPost.addHeader(Header.OZONE_USER,
        UserGroupInformation.getCurrentUser().getUserName());
    httpPost.addHeader(HttpHeaders.AUTHORIZATION,
        Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " "
            + OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    return httpPost;
  }

}
