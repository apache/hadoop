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
package org.apache.hadoop.fs.cosn.auth;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import com.qcloud.cos.auth.AnonymousCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a list of cos credentials provider.
 */
public class COSCredentialsProviderList implements
    COSCredentialsProvider, AutoCloseable {
  private static final Logger LOG =
      LoggerFactory.getLogger(COSCredentialsProviderList.class);

  private static final String NO_COS_CREDENTIAL_PROVIDERS =
      "No COS Credential Providers";
  private static final String CREDENTIALS_REQUESTED_WHEN_CLOSED =
      "Credentials requested after provider list was closed";

  private final List<COSCredentialsProvider> providers =
      new ArrayList<COSCredentialsProvider>(1);
  private boolean reuseLastProvider = true;
  private COSCredentialsProvider lastProvider;

  private final AtomicInteger refCount = new AtomicInteger(1);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public COSCredentialsProviderList() {
  }

  public COSCredentialsProviderList(
      Collection<COSCredentialsProvider> providers) {
    this.providers.addAll(providers);
  }

  public void add(COSCredentialsProvider provider) {
    this.providers.add(provider);
  }

  public int getRefCount() {
    return this.refCount.get();
  }

  public void checkNotEmpty() {
    if (this.providers.isEmpty()) {
      throw new NoAuthWithCOSException(NO_COS_CREDENTIAL_PROVIDERS);
    }
  }

  public COSCredentialsProviderList share() {
    Preconditions.checkState(!this.closed(), "Provider list is closed");
    this.refCount.incrementAndGet();
    return this;
  }

  public boolean closed() {
    return this.isClosed.get();
  }

  @Override
  public COSCredentials getCredentials() {
    if (this.closed()) {
      throw new NoAuthWithCOSException(CREDENTIALS_REQUESTED_WHEN_CLOSED);
    }

    this.checkNotEmpty();

    if (this.reuseLastProvider && this.lastProvider != null) {
      return this.lastProvider.getCredentials();
    }

    for (COSCredentialsProvider provider : this.providers) {
      COSCredentials credentials = provider.getCredentials();
      if (null != credentials
           && !StringUtils.isNullOrEmpty(credentials.getCOSAccessKeyId())
           && !StringUtils.isNullOrEmpty(credentials.getCOSSecretKey())
           || credentials instanceof AnonymousCOSCredentials) {
        this.lastProvider = provider;
        return credentials;
      }
    }

    throw new NoAuthWithCOSException(
        "No COS Credentials provided by " + this.providers.toString());
  }

  @Override
  public void refresh() {
    if (this.closed()) {
      return;
    }

    for (COSCredentialsProvider cosCredentialsProvider : this.providers) {
      cosCredentialsProvider.refresh();
    }
  }

  @Override
  public void close() throws Exception {
    if (this.closed()) {
      return;
    }

    int remainder = this.refCount.decrementAndGet();
    if (remainder != 0) {
      return;
    }
    this.isClosed.set(true);

    for (COSCredentialsProvider provider : this.providers) {
      if (provider instanceof Closeable) {
        ((Closeable) provider).close();
      }
    }
  }
}
