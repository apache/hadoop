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
package org.apache.hadoop.ozone.s3;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3TOKEN;
import static org.apache.hadoop.ozone.s3.AWSAuthParser.AUTHORIZATION_HEADER;
import static org.apache.hadoop.ozone.s3.AWSAuthParser.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.AUTH_PROTOCOL_NOT_SUPPORTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_TOKEN_CREATION_ERROR;

/**
 * This class creates the OzoneClient for the Rest endpoints.
 */
@RequestScoped
public class OzoneClientProducer {

  private final static Logger LOG =
      LoggerFactory.getLogger(OzoneClientProducer.class);

  @Context
  private ContainerRequestContext context;

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Inject
  private Text omService;


  @Produces
  public OzoneClient createClient() throws IOException {
    return getClient(ozoneConfiguration);
  }

  private OzoneClient getClient(OzoneConfiguration config) throws IOException {
    try {
      if (OzoneSecurityUtil.isSecurityEnabled(config)) {
        LOG.debug("Creating s3 token for client.");
        if (context.getHeaderString(AUTHORIZATION_HEADER).startsWith("AWS4")) {
          try {
            AWSV4AuthParser v4RequestParser = new AWSV4AuthParser(context);
            v4RequestParser.parse();

            OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
            identifier.setTokenType(S3TOKEN);
            identifier.setStrToSign(v4RequestParser.getStringToSign());
            identifier.setSignature(v4RequestParser.getSignature());
            identifier.setAwsAccessId(v4RequestParser.getAwsAccessId());
            identifier.setOwner(new Text(v4RequestParser.getAwsAccessId()));

            LOG.trace("Adding token for service:{}", omService);
            Token<OzoneTokenIdentifier> token = new Token(identifier.getBytes(),
                identifier.getSignature().getBytes(UTF_8),
                identifier.getKind(),
                omService);
            UserGroupInformation remoteUser =
                UserGroupInformation.createRemoteUser(
                    v4RequestParser.getAwsAccessId());
            remoteUser.addToken(token);
            UserGroupInformation.setLoginUser(remoteUser);
          } catch (OS3Exception | URISyntaxException ex) {
            LOG.error("S3 token creation failed.");
            throw S3_TOKEN_CREATION_ERROR;
          }
        } else {
          throw AUTH_PROTOCOL_NOT_SUPPORTED;
        }
      }
    } catch (Exception e) {
      LOG.error("Error: ", e);
    }
    return OzoneClientFactory.getClient(ozoneConfiguration);
  }

  @VisibleForTesting
  public void setContext(ContainerRequestContext context) {
    this.context = context;
  }

  @VisibleForTesting
  public void setOzoneConfiguration(OzoneConfiguration config) {
    this.ozoneConfiguration = config;
  }
}
