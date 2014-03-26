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

package org.apache.hadoop.tools;

import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;

public class TestDelegationTokenRemoteFetcher {
  private static final Logger LOG = Logger
      .getLogger(TestDelegationTokenRemoteFetcher.class);

  private static final String EXP_DATE = "124123512361236";
  private static final String tokenFile = "http.file.dta";
  private static final URLConnectionFactory connectionFactory = URLConnectionFactory.DEFAULT_SYSTEM_CONNECTION_FACTORY;

  private int httpPort;
  private URI serviceUrl;
  private FileSystem fileSys;
  private Configuration conf;
  private ServerBootstrap bootstrap;
  private Token<DelegationTokenIdentifier> testToken;
  private volatile AssertionError assertionError;
  
  @Before
  public void init() throws Exception {
    conf = new Configuration();
    fileSys = FileSystem.getLocal(conf);
    httpPort = NetUtils.getFreeSocketPort();
    serviceUrl = new URI("http://localhost:" + httpPort);
    testToken = createToken(serviceUrl);
  }

  @After
  public void clean() throws IOException {
    if (fileSys != null)
      fileSys.delete(new Path(tokenFile), true);
    if (bootstrap != null)
      bootstrap.releaseExternalResources();
  }

  /**
   * try to fetch token without http server with IOException
   */
  @Test
  public void testTokenFetchFail() throws Exception {
    try {
      DelegationTokenFetcher.main(new String[] { "-webservice=" + serviceUrl,
          tokenFile });
      fail("Token fetcher shouldn't start in absense of NN");
    } catch (IOException ex) {
    }
  }
  
  /**
   * try to fetch token without http server with IOException
   */
  @Test
  public void testTokenRenewFail() throws AuthenticationException {
    try {
      DelegationTokenFetcher.renewDelegationToken(connectionFactory, serviceUrl, testToken);
      fail("Token fetcher shouldn't be able to renew tokens in absense of NN");
    } catch (IOException ex) {
    } 
  }     
  
  /**
   * try cancel token without http server with IOException
   */
  @Test
  public void expectedTokenCancelFail() throws AuthenticationException {
    try {
      DelegationTokenFetcher.cancelDelegationToken(connectionFactory, serviceUrl, testToken);
      fail("Token fetcher shouldn't be able to cancel tokens in absense of NN");
    } catch (IOException ex) {
    } 
  }
  
  /**
   * try fetch token and get http response with error
   */
  @Test  
  public void expectedTokenRenewErrorHttpResponse()
      throws AuthenticationException, URISyntaxException {
    bootstrap = startHttpServer(httpPort, testToken, serviceUrl);
    try {
      DelegationTokenFetcher.renewDelegationToken(connectionFactory, new URI(
          serviceUrl.toString() + "/exception"), createToken(serviceUrl));
      fail("Token fetcher shouldn't be able to renew tokens using an invalid"
          + " NN URL");
    } catch (IOException ex) {
    } 
    if (assertionError != null)
      throw assertionError;
  }
  
  /**
   *
   */
  @Test
  public void testCancelTokenFromHttp() throws IOException,
      AuthenticationException {
    bootstrap = startHttpServer(httpPort, testToken, serviceUrl);
    DelegationTokenFetcher.cancelDelegationToken(connectionFactory, serviceUrl,
        testToken);
    if (assertionError != null)
      throw assertionError;
  }
  
  /**
   * Call renew token using http server return new expiration time
   */
  @Test
  public void testRenewTokenFromHttp() throws IOException,
      NumberFormatException, AuthenticationException {
    bootstrap = startHttpServer(httpPort, testToken, serviceUrl);
    assertTrue("testRenewTokenFromHttp error",
        Long.parseLong(EXP_DATE) == DelegationTokenFetcher.renewDelegationToken(
            connectionFactory, serviceUrl, testToken));
    if (assertionError != null)
      throw assertionError;
  }

  /**
   * Call fetch token using http server 
   */
  @Test
  public void expectedTokenIsRetrievedFromHttp() throws Exception {
    bootstrap = startHttpServer(httpPort, testToken, serviceUrl);
    DelegationTokenFetcher.main(new String[] { "-webservice=" + serviceUrl,
        tokenFile });
    Path p = new Path(fileSys.getWorkingDirectory(), tokenFile);
    Credentials creds = Credentials.readTokenStorageFile(p, conf);
    Iterator<Token<?>> itr = creds.getAllTokens().iterator();
    assertTrue("token not exist error", itr.hasNext());
    Token<?> fetchedToken = itr.next();
    Assert.assertArrayEquals("token wrong identifier error",
        testToken.getIdentifier(), fetchedToken.getIdentifier());
    Assert.assertArrayEquals("token wrong password error",
        testToken.getPassword(), fetchedToken.getPassword());
    if (assertionError != null)
      throw assertionError;
  }
  
  private static Token<DelegationTokenIdentifier> createToken(URI serviceUri) {
    byte[] pw = "hadoop".getBytes();
    byte[] ident = new DelegationTokenIdentifier(new Text("owner"), new Text(
        "renewer"), new Text("realuser")).getBytes();
    Text service = new Text(serviceUri.toString());
    return new Token<DelegationTokenIdentifier>(ident, pw,
        HftpFileSystem.TOKEN_KIND, service);
  }

  private interface Handler {
    void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException;
  }

  private class FetchHandler implements Handler {
    
    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      Assert.assertEquals(testToken, token);

      Credentials creds = new Credentials();
      creds.addToken(new Text(serviceUrl), token);
      DataOutputBuffer out = new DataOutputBuffer();
      creds.write(out);
      int fileLength = out.getData().length;
      ChannelBuffer cbuffer = ChannelBuffers.buffer(fileLength);
      cbuffer.writeBytes(out.getData());
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
          String.valueOf(fileLength));
      response.setContent(cbuffer);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }
  }

  private class RenewHandler implements Handler {
    
    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      Assert.assertEquals(testToken, token);
      byte[] bytes = EXP_DATE.getBytes();
      ChannelBuffer cbuffer = ChannelBuffers.buffer(bytes.length);
      cbuffer.writeBytes(bytes);
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
          String.valueOf(bytes.length));
      response.setContent(cbuffer);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }
  }
  
  private class ExceptionHandler implements Handler {

    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      Assert.assertEquals(testToken, token);
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, 
          HttpResponseStatus.METHOD_NOT_ALLOWED);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }    
  }
  
  private class CancelHandler implements Handler {

    @Override
    public void handle(Channel channel, Token<DelegationTokenIdentifier> token,
        String serviceUrl) throws IOException {
      Assert.assertEquals(testToken, token);
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }    
  }
  
  private final class CredentialsLogicHandler extends
      SimpleChannelUpstreamHandler {

    private final Token<DelegationTokenIdentifier> token;
    private final String serviceUrl;
    private final ImmutableMap<String, Handler> routes = ImmutableMap.of(
        "/exception", new ExceptionHandler(),
        "/cancelDelegationToken", new CancelHandler(),
        "/getDelegationToken", new FetchHandler() , 
        "/renewDelegationToken", new RenewHandler());

    public CredentialsLogicHandler(Token<DelegationTokenIdentifier> token,
        String serviceUrl) {
      this.token = token;
      this.serviceUrl = serviceUrl;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e)
        throws Exception {
      HttpRequest request = (HttpRequest) e.getMessage();

      if (request.getMethod() == HttpMethod.OPTIONS) {
        // Mimic SPNEGO authentication
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1,
            HttpResponseStatus.OK);
        response.addHeader("Set-Cookie", "hadoop-auth=1234");
        e.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
      } else if (request.getMethod() != GET) {
        e.getChannel().close();
      }
      UnmodifiableIterator<Map.Entry<String, Handler>> iter = routes.entrySet()
          .iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Handler> entry = iter.next();
        if (request.getUri().contains(entry.getKey())) {
          Handler handler = entry.getValue();
          try {
            handler.handle(e.getChannel(), token, serviceUrl);
          } catch (AssertionError ee) {
            TestDelegationTokenRemoteFetcher.this.assertionError = ee;
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, 
                HttpResponseStatus.BAD_REQUEST);
            response.setContent(ChannelBuffers.copiedBuffer(ee.getMessage(), 
                Charset.defaultCharset()));
            e.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
          }
          return;
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      Channel ch = e.getChannel();
      Throwable cause = e.getCause();

      if (LOG.isDebugEnabled())
        LOG.debug(cause.getMessage());
      ch.close().addListener(ChannelFutureListener.CLOSE);
    }
  }

  private ServerBootstrap startHttpServer(int port,
      final Token<DelegationTokenIdentifier> token, final URI url) {
    ServerBootstrap bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(new HttpRequestDecoder(),
            new HttpChunkAggregator(65536), new HttpResponseEncoder(),
            new CredentialsLogicHandler(token, url.toString()));
      }
    });
    bootstrap.bind(new InetSocketAddress("localhost", port));
    return bootstrap;
  }
  
}
