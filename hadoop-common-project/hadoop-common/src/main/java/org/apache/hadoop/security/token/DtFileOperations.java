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

package org.apache.hadoop.security.token;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ServiceLoader;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/**
 * DtFileOperations is a collection of delegation token file operations.
 */
public final class DtFileOperations {
  private static final Log LOG = LogFactory.getLog(DtFileOperations.class);

  /** No public constructor as per checkstyle. */
  private DtFileOperations() { }

  /**
   * Use FORMAT_* as arguments to format parameters.
   * FORMAT_PB is for protobuf output.
   */
  public static final String FORMAT_PB = "protobuf";

  /**
   * Use FORMAT_* as arguments to format parameters.
   * FORMAT_JAVA is a legacy option for java serialization output.
   */
  public static final String FORMAT_JAVA = "java";

  private static final String NA_STRING = "-NA-";
  private static final String PREFIX_HTTP = "http://";
  private static final String PREFIX_HTTPS = "https://";

  /** Let the DtFetcher code add the appropriate prefix if HTTP/S is used. */
  private static String stripPrefix(String u) {
    return u.replaceFirst(PREFIX_HTTP, "").replaceFirst(PREFIX_HTTPS, "");
  }

  /** Match token service field to alias text.  True if alias is null. */
  private static boolean matchAlias(Token<?> token, Text alias) {
    return alias == null || token.getService().equals(alias);
  }

  /** Match fetcher's service name to the service text and/or url prefix. */
  private static boolean matchService(
      DtFetcher fetcher, Text service, String url) {
    Text sName = fetcher.getServiceName();
    return (service == null && url.startsWith(sName.toString() + "://")) ||
           (service != null && service.equals(sName));
  }

  /** Format a long integer type into a date string. */
  private static String formatDate(long date) {
    DateFormat df = DateFormat.getDateTimeInstance(
        DateFormat.SHORT, DateFormat.SHORT);
    return df.format(new Date(date));
  }

  /** Add the service prefix for a local filesystem. */
  private static Path fileToPath(File f) {
    return new Path("file:" + f.getAbsolutePath());
  }

  /** Write out a Credentials object as a local file.
   *  @param f a local File object.
   *  @param format a string equal to FORMAT_PB or FORMAT_JAVA.
   *  @param creds the Credentials object to be written out.
   *  @param conf a Configuration object passed along.
   *  @throws IOException
   */
  public static void doFormattedWrite(
      File f, String format, Credentials creds, Configuration conf)
      throws IOException {
    if (format == null || format.equals(FORMAT_PB)) {
      creds.writeTokenStorageFile(fileToPath(f), conf);
    } else { // if (format != null && format.equals(FORMAT_JAVA)) {
      creds.writeLegacyTokenStorageLocalFile(f);
    }
  }

  /** Print out a Credentials file from the local filesystem.
   *  @param tokenFile a local File object.
   *  @param alias print only tokens matching alias (null matches all).
   *  @param conf Configuration object passed along.
   *  @param out print to this stream.
   *  @throws IOException
   */
  public static void printTokenFile(
      File tokenFile, Text alias, Configuration conf, PrintStream out)
      throws IOException {
    out.println("File: " + tokenFile.getPath());
    Credentials creds = Credentials.readTokenStorageFile(tokenFile, conf);
    printCredentials(creds, alias, out);
  }

  /** Print out a Credentials object.
   *  @param creds the Credentials object to be printed out.
   *  @param alias print only tokens matching alias (null matches all).
   *  @param out print to this stream.
   *  @throws IOException
   */
  public static void printCredentials(
      Credentials creds, Text alias, PrintStream out)
      throws IOException {
    boolean tokenHeader = true;
    String fmt = "%-24s %-20s %-15s %-12s %s%n";
    for (Token<?> token : creds.getAllTokens()) {
      if (matchAlias(token, alias)) {
        if (tokenHeader) {
          out.printf(fmt, "Token kind", "Service", "Renewer", "Exp date",
                     "URL enc token");
          out.println(StringUtils.repeat("-", 80));
          tokenHeader = false;
        }
        AbstractDelegationTokenIdentifier id =
            (AbstractDelegationTokenIdentifier) token.decodeIdentifier();
        out.printf(fmt, token.getKind(), token.getService(),
                   (id != null) ? id.getRenewer() : NA_STRING,
                   (id != null) ? formatDate(id.getMaxDate()) : NA_STRING,
                   token.encodeToUrlString());
      }
    }
  }

  /** Fetch a token from a service and save to file in the local filesystem.
   *  @param tokenFile a local File object to hold the output.
   *  @param fileFormat a string equal to FORMAT_PB or FORMAT_JAVA, for output
   *  @param alias overwrite service field of fetched token with this text.
   *  @param service use a DtFetcher implementation matching this service text.
   *  @param url pass this URL to fetcher after stripping any http/s prefix.
   *  @param renewer pass this renewer to the fetcher.
   *  @param conf Configuration object passed along.
   *  @throws IOException
   */
  public static void getTokenFile(File tokenFile, String fileFormat,
      Text alias, Text service, String url, String renewer, Configuration conf)
      throws Exception {
    Token<?> token = null;
    Credentials creds = tokenFile.exists() ?
        Credentials.readTokenStorageFile(tokenFile, conf) : new Credentials();
    ServiceLoader<DtFetcher> loader = ServiceLoader.load(DtFetcher.class);
    for (DtFetcher fetcher : loader) {
      if (matchService(fetcher, service, url)) {
        if (!fetcher.isTokenRequired()) {
          String message = "DtFetcher for service '" + service +
              "' does not require a token.  Check your configuration.  " +
              "Note: security may be disabled or there may be two DtFetcher " +
              "providers for the same service designation.";
          LOG.error(message);
          throw new IllegalArgumentException(message);
        }
        token = fetcher.addDelegationTokens(conf, creds, renewer,
                                            stripPrefix(url));
      }
    }
    if (alias != null) {
      if (token == null) {
        String message = "DtFetcher for service '" + service + "'" +
            " does not allow aliasing.  Cannot apply alias '" + alias + "'." +
            "  Drop alias flag to get token for this service.";
        LOG.error(message);
        throw new IOException(message);
      }
      Token<?> aliasedToken = token.copyToken();
      aliasedToken.setService(alias);
      creds.addToken(alias, aliasedToken);
      LOG.info("Add token with service " + alias);
    }
    doFormattedWrite(tokenFile, fileFormat, creds, conf);
  }

  /** Alias a token from a file and save back to file in the local filesystem.
   *  @param tokenFile a local File object to hold the input and output.
   *  @param fileFormat a string equal to FORMAT_PB or FORMAT_JAVA, for output
   *  @param alias overwrite service field of fetched token with this text.
   *  @param service only apply alias to tokens matching this service text.
   *  @param conf Configuration object passed along.
   *  @throws IOException
   */
  public static void aliasTokenFile(File tokenFile, String fileFormat,
      Text alias, Text service, Configuration conf) throws Exception {
    Credentials newCreds = new Credentials();
    Credentials creds = Credentials.readTokenStorageFile(tokenFile, conf);
    for (Token<?> token : creds.getAllTokens()) {
      newCreds.addToken(token.getService(), token);
      if (token.getService().equals(service)) {
        Token<?> aliasedToken = token.copyToken();
        aliasedToken.setService(alias);
        newCreds.addToken(alias, aliasedToken);
      }
    }
    doFormattedWrite(tokenFile, fileFormat, newCreds, conf);
  }

  /** Append tokens from list of files in local filesystem, saving to last file.
   *  @param tokenFiles list of local File objects.  Last file holds the output.
   *  @param fileFormat a string equal to FORMAT_PB or FORMAT_JAVA, for output
   *  @param conf Configuration object passed along.
   *  @throws IOException
   */
  public static void appendTokenFiles(
      ArrayList<File> tokenFiles, String fileFormat, Configuration conf)
      throws IOException {
    Credentials newCreds = new Credentials();
    File lastTokenFile = null;
    for (File tokenFile : tokenFiles) {
      lastTokenFile = tokenFile;
      Credentials creds = Credentials.readTokenStorageFile(tokenFile, conf);
      for (Token<?> token : creds.getAllTokens()) {
        newCreds.addToken(token.getService(), token);
      }
    }
    doFormattedWrite(lastTokenFile, fileFormat, newCreds, conf);
  }

  /** Remove a token from a file in the local filesystem, matching alias.
   *  @param cancel cancel token as well as remove from file.
   *  @param tokenFile a local File object.
   *  @param fileFormat a string equal to FORMAT_PB or FORMAT_JAVA, for output
   *  @param alias remove only tokens matching alias; null matches all.
   *  @param conf Configuration object passed along.
   *  @throws IOException
   *  @throws InterruptedException
   */
  public static void removeTokenFromFile(boolean cancel,
      File tokenFile, String fileFormat, Text alias, Configuration conf)
      throws IOException, InterruptedException {
    Credentials newCreds = new Credentials();
    Credentials creds = Credentials.readTokenStorageFile(tokenFile, conf);
    for (Token<?> token : creds.getAllTokens()) {
      if (matchAlias(token, alias)) {
        if (token.isManaged() && cancel) {
          token.cancel(conf);
          LOG.info("Canceled " + token.getKind() + ":" + token.getService());
        }
      } else {
        newCreds.addToken(token.getService(), token);
      }
    }
    doFormattedWrite(tokenFile, fileFormat, newCreds, conf);
  }

  /** Renew a token from a file in the local filesystem, matching alias.
   *  @param tokenFile a local File object.
   *  @param fileFormat a string equal to FORMAT_PB or FORMAT_JAVA, for output
   *  @param alias renew only tokens matching alias; null matches all.
   *  @param conf Configuration object passed along.
   *  @throws IOException
   *  @throws InterruptedException
   */
  public static void renewTokenFile(
      File tokenFile, String fileFormat, Text alias, Configuration conf)
      throws IOException, InterruptedException {
    Credentials creds = Credentials.readTokenStorageFile(tokenFile, conf);
    for (Token<?> token : creds.getAllTokens()) {
      if (token.isManaged() && matchAlias(token, alias)) {
        long result = token.renew(conf);
        LOG.info("Renewed" + token.getKind() + ":" + token.getService() +
                 " until " + formatDate(result));
      }
    }
    doFormattedWrite(tokenFile, fileFormat, creds, conf);
  }
}
