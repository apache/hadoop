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

package org.apache.hadoop.yarn.util;

import static org.apache.hadoop.yarn.util.StringHelper._split;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;


/**
 * This class contains a set of utilities which help converting data structures
 * from/to 'serializableFormat' to/from hadoop/nativejava data structures.
 *
 */
public class ConverterUtils {

  public static final String APPLICATION_PREFIX = "application";
  public static final String CONTAINER_PREFIX = "container";
  public static final String APPLICATION_ATTEMPT_PREFIX = "appattempt";

  /**
   * return a hadoop path from a given url
   * 
   * @param url
   *          url to convert
   * @return path from {@link URL}
   * @throws URISyntaxException
   */
  public static Path getPathFromYarnURL(URL url) throws URISyntaxException {
    String scheme = url.getScheme() == null ? "" : url.getScheme();
    String authority = url.getHost() != null ? url.getHost() + ":" + url.getPort()
        : "";
    return new Path(
        (new URI(scheme, authority, url.getFile(), null, null)).normalize());
  }
  
  /**
   * change from CharSequence to string for map key and value
   * @param env map for converting
   * @return string,string map
   */
  public static Map<String, String> convertToString(
      Map<CharSequence, CharSequence> env) {
    
    Map<String, String> stringMap = new HashMap<String, String>();
    for (Entry<CharSequence, CharSequence> entry: env.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
   }

  public static URL getYarnUrlFromPath(Path path) {
    return getYarnUrlFromURI(path.toUri());
  }
  
  public static URL getYarnUrlFromURI(URI uri) {
    URL url = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(URL.class);
    if (uri.getHost() != null) {
      url.setHost(uri.getHost());
    }
    url.setPort(uri.getPort());
    url.setScheme(uri.getScheme());
    url.setFile(uri.getPath());
    return url;
  }

  // TODO: Why thread local?
  // ^ NumberFormat instances are not threadsafe
  private static final ThreadLocal<NumberFormat> appIdFormat =
    new ThreadLocal<NumberFormat>() {
      @Override
      public NumberFormat initialValue() {
        NumberFormat fmt = NumberFormat.getInstance();
        fmt.setGroupingUsed(false);
        fmt.setMinimumIntegerDigits(4);
        return fmt;
      }
    };

  

  public static String toString(ApplicationId appId) {
    StringBuilder sb = new StringBuilder();
    sb.append(APPLICATION_PREFIX + "_").append(appId.getClusterTimestamp())
        .append("_");
    sb.append(appIdFormat.get().format(appId.getId()));
    return sb.toString();
  }

  public static ApplicationId toApplicationId(RecordFactory recordFactory,
      String appIdStr) {
    Iterator<String> it = _split(appIdStr).iterator();
    it.next(); // prefix. TODO: Validate application prefix
    return toApplicationId(recordFactory, it);
  }

  private static ApplicationId toApplicationId(RecordFactory recordFactory,
      Iterator<String> it) {
    ApplicationId appId =
        recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(Long.parseLong(it.next()));
    appId.setId(Integer.parseInt(it.next()));
    return appId;
  }

  private static ApplicationAttemptId toApplicationAttemptId(
      Iterator<String> it) throws NumberFormatException {
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(Long.parseLong(it.next()));
    appId.setId(Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId = Records
        .newRecord(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(Integer.parseInt(it.next()));
    return appAttemptId;
  }

  public static String toString(ContainerId cId) {
    return cId.toString();
  }

  public static ContainerId toContainerId(String containerIdStr)
      throws IOException {
    Iterator<String> it = _split(containerIdStr).iterator();
    if (!it.next().equals(CONTAINER_PREFIX)) {
      throw new IOException("Invalid ContainerId prefix: " + containerIdStr);
    }
    try {
      ApplicationAttemptId appAttemptID = toApplicationAttemptId(it);
      ContainerId containerId = Records.newRecord(ContainerId.class);
      containerId.setApplicationAttemptId(appAttemptID);
      containerId.setId(Integer.parseInt(it.next()));
      return containerId;
    } catch (NumberFormatException n) {
      throw new IOException("Invalid ContainerId: " + containerIdStr, n);
    }
  }

  public static ApplicationAttemptId toApplicationAttemptId(
      String applicationAttmeptIdStr) throws IOException {
    Iterator<String> it = _split(applicationAttmeptIdStr).iterator();
    if (!it.next().equals(APPLICATION_ATTEMPT_PREFIX)) {
      throw new IOException("Invalid AppAttemptId prefix: "
          + applicationAttmeptIdStr);
    }
    try {
      return toApplicationAttemptId(it);
    } catch (NumberFormatException n) {
      throw new IOException("Invalid AppAttemptId: "
          + applicationAttmeptIdStr, n);
    }
  }
}
