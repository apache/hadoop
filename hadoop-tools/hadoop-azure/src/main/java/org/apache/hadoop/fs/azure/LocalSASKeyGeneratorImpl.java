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

package org.apache.hadoop.fs.azure;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.SharedAccessAccountPermissions;
import com.microsoft.azure.storage.SharedAccessAccountPolicy;
import com.microsoft.azure.storage.SharedAccessAccountResourceType;
import com.microsoft.azure.storage.SharedAccessAccountService;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/***
 * Local SAS Key Generation implementation. This class resides in
 * the same address space as the WASB driver.
 *
 * This class gets typically used for testing purposes.
 *
 */

public class LocalSASKeyGeneratorImpl extends SASKeyGeneratorImpl {

  /**
   * Map to cache CloudStorageAccount instances.
   */
  private Map<String, CloudStorageAccount> storageAccountMap;
  private CachingAuthorizer<CachedSASKeyEntry, URI> cache;
  private static final int HOURS_IN_DAY = 24;

  public LocalSASKeyGeneratorImpl(Configuration conf) {
    super(conf);
    storageAccountMap = new HashMap<String, CloudStorageAccount>();
    cache = new CachingAuthorizer<>(getSasKeyExpiryPeriod(), "SASKEY");
    cache.init(conf);
  }

  /**
   * Implementation to generate SAS Key for a container
   */
  @Override
  public URI getContainerSASUri(String accountName, String container)
      throws SASKeyGenerationException {

    try {

      CachedSASKeyEntry cacheKey = new CachedSASKeyEntry(accountName, container, "/");
      URI cacheResult = cache.get(cacheKey);
      if (cacheResult != null) {
        return cacheResult;
      }

      CloudStorageAccount account =
          getSASKeyBasedStorageAccountInstance(accountName);
      CloudBlobClient client = account.createCloudBlobClient();
      URI sasKey = client.getCredentials().transformUri(
          client.getContainerReference(container).getUri());
      cache.put(cacheKey, sasKey);
      return sasKey;

    } catch (StorageException stoEx) {
      throw new SASKeyGenerationException("Encountered StorageException while"
          + " generating SAS Key for container " + container + " inside "
              + "storage account " + accountName, stoEx);
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException while"
          + " generating SAS Key for container " + container + " inside storage"
              + " account " + accountName, uriSyntaxEx);
    }
  }

  /**
   * Helper method that creates a CloudStorageAccount instance based on
   *  SAS key for accountName
   *
   * @param accountName Storage Account Name
   * @return CloudStorageAccount instance created using SAS key for
   *   the Storage Account.
   * @throws SASKeyGenerationException
   */
  private CloudStorageAccount getSASKeyBasedStorageAccountInstance(
      String accountName) throws SASKeyGenerationException {

    try {

      String accountNameWithoutDomain =
          getAccountNameWithoutDomain(accountName);

      CloudStorageAccount account =
          getStorageAccountInstance(accountNameWithoutDomain,
              AzureNativeFileSystemStore.getAccountKeyFromConfiguration(
                  accountName, getConf()));

      return new CloudStorageAccount(
          new StorageCredentialsSharedAccessSignature(
              account.generateSharedAccessSignature(
                  getDefaultAccountAccessPolicy())), false,
                  account.getEndpointSuffix(), accountNameWithoutDomain);

    } catch (KeyProviderException keyProviderEx) {
      throw new SASKeyGenerationException("Encountered KeyProviderException"
          + " while retrieving Storage key from configuration for account "
          + accountName, keyProviderEx);
    } catch (InvalidKeyException invalidKeyEx) {
      throw new SASKeyGenerationException("Encoutered InvalidKeyException "
          + "while generating Account level SAS key for account" + accountName,
          invalidKeyEx);
    } catch(StorageException storeEx) {
      throw new SASKeyGenerationException("Encoutered StorageException while "
          + "generating Account level SAS key for account" + accountName,
            storeEx);
    } catch(URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException for"
          + " account " + accountName, uriSyntaxEx);
    }
  }

  /**
   * Implementation for generation of Relative Path Blob SAS Uri.
   */
  @Override
  public URI getRelativeBlobSASUri(String accountName, String container,
      String relativePath) throws SASKeyGenerationException {

    CloudBlobContainer sc = null;
    CloudBlobClient client = null;
    CachedSASKeyEntry cacheKey = null;

    try {

      cacheKey = new CachedSASKeyEntry(accountName, container, relativePath);
      URI cacheResult = cache.get(cacheKey);
      if (cacheResult != null) {
        return cacheResult;
      }

      CloudStorageAccount account =
          getSASKeyBasedStorageAccountInstance(accountName);
      client = account.createCloudBlobClient();
      sc = client.getContainerReference(container);
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException "
          + "while getting container references for container " + container
          + " inside storage account : " + accountName, uriSyntaxEx);
    } catch (StorageException stoEx) {
      throw new SASKeyGenerationException("Encountered StorageException while "
          + "getting  container references for container " + container
          + " inside storage account : " + accountName, stoEx);
    }

    CloudBlockBlob blob = null;
    try {
      blob = sc.getBlockBlobReference(relativePath);
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException while "
          + "getting Block Blob references for container " + container
          + " inside storage account : " + accountName, uriSyntaxEx);
    } catch (StorageException stoEx) {
      throw new SASKeyGenerationException("Encountered StorageException while "
          + "getting Block Blob references for container " + container
          + " inside storage account : " + accountName, stoEx);
    }

    try {
      URI sasKey = client.getCredentials().transformUri(blob.getUri());
      cache.put(cacheKey, sasKey);
      return sasKey;
    } catch (StorageException stoEx) {
      throw new SASKeyGenerationException("Encountered StorageException while "
          + "generating SAS key for Blob: " + relativePath + " inside "
          + "container : " + container + " in Storage Account : " + accountName,
              stoEx);
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException "
          + "while generating SAS key for Blob: " + relativePath + " inside "
          + "container: " + container + " in Storage Account : " + accountName,
              uriSyntaxEx);
    }
  }

  /**
   * Helper method that creates CloudStorageAccount Instance using the
   * storage account key.
   * @param accountName Name of the storage account
   * @param accountKey Storage Account key
   * @return CloudStorageAccount instance for the storage account.
   * @throws SASKeyGenerationException
   */
  private CloudStorageAccount getStorageAccountInstance(String accountName,
      String accountKey) throws SASKeyGenerationException {

    if (!storageAccountMap.containsKey(accountName)) {

      CloudStorageAccount account = null;
      try {
        account =
            new CloudStorageAccount(new StorageCredentialsAccountAndKey(
                accountName, accountKey));
      } catch (URISyntaxException uriSyntaxEx) {
        throw new SASKeyGenerationException("Encountered URISyntaxException "
            + "for account " + accountName, uriSyntaxEx);
      }

      storageAccountMap.put(accountName, account);
    }

    return storageAccountMap.get(accountName);
  }

  /**
   * Helper method that returns the Storage account name without
   * the domain name suffix.
   * @param fullAccountName Storage account name with domain name suffix
   * @return String
   */
  private String getAccountNameWithoutDomain(String fullAccountName) {
    StringTokenizer tokenizer = new StringTokenizer(fullAccountName, ".");
    return tokenizer.nextToken();
  }

  /**
   * Helper method to generate Access Policy for the Storage Account SAS Key
   * @return SharedAccessAccountPolicy
   */
  private SharedAccessAccountPolicy getDefaultAccountAccessPolicy() {

    SharedAccessAccountPolicy ap =
        new SharedAccessAccountPolicy();

    Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    cal.setTime(new Date());
    cal.add(Calendar.HOUR, (int) getSasKeyExpiryPeriod() * HOURS_IN_DAY);

    ap.setSharedAccessExpiryTime(cal.getTime());
    ap.setPermissions(getDefaultAccoutSASKeyPermissions());
    ap.setResourceTypes(EnumSet.of(SharedAccessAccountResourceType.CONTAINER,
        SharedAccessAccountResourceType.OBJECT));
    ap.setServices(EnumSet.of(SharedAccessAccountService.BLOB));

    return ap;
  }

  private EnumSet<SharedAccessAccountPermissions> getDefaultAccoutSASKeyPermissions() {
    return EnumSet.of(SharedAccessAccountPermissions.ADD,
        SharedAccessAccountPermissions.CREATE,
        SharedAccessAccountPermissions.DELETE,
        SharedAccessAccountPermissions.LIST,
        SharedAccessAccountPermissions.READ,
        SharedAccessAccountPermissions.UPDATE,
        SharedAccessAccountPermissions.WRITE);
  }
}