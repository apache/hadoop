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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.auth.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;

import org.apache.commons.lang.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * A subclass of {@link InstanceProfileCredentialsProvider} that enforces
 * instantiation of only a single instance.
 * This credential provider calls the EC2 instance metadata service to obtain
 * credentials.  For highly multi-threaded applications, it's possible that
 * multiple instances call the service simultaneously and overwhelm it with
 * load.  The service handles this by throttling the client with an HTTP 429
 * response or forcibly terminating the connection.  Forcing use of a single
 * instance reduces load on the metadata service by allowing all threads to
 * share the credentials.  The base class is thread-safe, and there is nothing
 * that varies in the credentials across different instances of
 * {@link S3AFileSystem} connecting to different buckets, so sharing a singleton
 * instance is safe.
 *
 * As of AWS SDK 1.11.39, the SDK code internally enforces a singleton.  After
 * Hadoop upgrades to that version or higher, it's likely that we can remove
 * this class.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class SharedInstanceProfileCredentialsProvider
    extends InstanceProfileCredentialsProvider {

  private static final SharedInstanceProfileCredentialsProvider INSTANCE =
      new SharedInstanceProfileCredentialsProvider();

  private static final Path s3crednetialPath = new Path("hdfs:///tmp/s3credential.txt");

  private volatile AWSCredentials credentials;

  private static final int maxRetries = 5;

  /**
   * Returns the singleton instance.
   *
   * @return singleton instance
   */
  public static SharedInstanceProfileCredentialsProvider getInstance() {
    return INSTANCE;
  }

  private AWSCredentials readCredentialsFromHDFS() {
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(s3crednetialPath)));
      String accessKey = br.readLine();
      String secretKey = br.readLine();
      String token = br.readLine();
      AWSCredentials credentials;
      if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
        // if there are no accessKey nor secretKey return null
        return null;
      } else if (StringUtils.isNotEmpty(token)) {
        credentials = new BasicSessionCredentials(accessKey, secretKey, token);
      } else {
        credentials = new BasicAWSCredentials(accessKey, secretKey);
      }
      return credentials;
    } catch (Exception e) {
      return null; // ignore the read errors
      // throw new AmazonServiceException("Failed reading S3 credentials from HDFS " + e.getStackTrace());
    }
  }

  private void writeCredentialsToHDFS(AWSCredentials credentials) {
    try {
      // Simulate atomic write by creating a new s3credential file with random string suffix and rename to s3crednetialPath
      Path newS3crednetialPath = new Path(s3crednetialPath.toUri() + RandomStringUtils.randomAlphanumeric(8));
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(newS3crednetialPath, true)));
      String accessKey = credentials.getAWSAccessKeyId();
      String secretKey = credentials.getAWSSecretKey();
      String token = "";
      if (credentials instanceof BasicSessionCredentials) {
        token = ((BasicSessionCredentials) credentials).getSessionToken();
      }
      br.write(accessKey);
      br.newLine();
      br.write(secretKey);
      br.newLine();
      br.write(token);
      br.newLine();
      br.close();
      fs.delete(s3crednetialPath, false);
      fs.rename(newS3crednetialPath, s3crednetialPath);
    } catch (Exception e) {
      // ignore write errors
      // throw new AmazonServiceException("Failed writing S3 credentials from HDFS " + e.getStackTrace());
    }
  }

  @Override
  public AWSCredentials getCredentials() {
    for (int retry = 0; retry < maxRetries; retry++) {
      try {
        AWSCredentials newCredentials = super.getCredentials();
        // if this new credentials is different from HDFS write back
        if (credentials == null || (!newCredentials.getAWSSecretKey().equals(credentials.getAWSSecretKey()))) {
          credentials = newCredentials;
          writeCredentialsToHDFS(credentials);
        }
        break;
      } catch (Exception e) {
        // fallback, read the credential from HDFS
        credentials = readCredentialsFromHDFS();
        if (credentials == null || StringUtils.isEmpty(credentials.getAWSAccessKeyId())
          || StringUtils.isEmpty(credentials.getAWSSecretKey())) {
          if (retry + 1 >= maxRetries) {
            throw e;
          } else {
            continue;
          }
        }
        break;
      }
    }
    return credentials;
  }

  /**
   * Default constructor, defined explicitly as private to enforce singleton.
   */
  private SharedInstanceProfileCredentialsProvider() {
    super();
  }
}
