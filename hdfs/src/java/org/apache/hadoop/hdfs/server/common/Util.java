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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class Util {
  private final static Log LOG = LogFactory.getLog(Util.class.getName());

  /**
   * Current system time.
   * @return current time in msec.
   */
  public static long now() {
    return System.currentTimeMillis();
  }
  
  /**
   * Interprets the passed string as a URI. In case of error it 
   * assumes the specified string is a file.
   *
   * @param s the string to interpret
   * @return the resulting URI 
   * @throws IOException 
   */
  public static URI stringAsURI(String s) throws IOException {
    URI u = null;
    // try to make a URI
    try {
      u = new URI(s);
    } catch (URISyntaxException e){
      LOG.error("Syntax error in URI " + s
          + ". Please check hdfs configuration.", e);
    }

    // if URI is null or scheme is undefined, then assume it's file://
    if(u == null || u.getScheme() == null){
      LOG.warn("Path " + s + " should be specified as a URI "
          + "in configuration files. Please update hdfs configuration.");
      u = fileAsURI(new File(s));
    }
    return u;
  }

  /**
   * Converts the passed File to a URI.
   *
   * @param f the file to convert
   * @return the resulting URI 
   * @throws IOException 
   */
  public static URI fileAsURI(File f) throws IOException {
    return f.getCanonicalFile().toURI();
  }

  /**
   * Converts a collection of strings into a collection of URIs.
   * @param names collection of strings to convert to URIs
   * @return collection of URIs
   */
  public static Collection<URI> stringCollectionAsURIs(
                                  Collection<String> names) {
    Collection<URI> uris = new ArrayList<URI>(names.size());
    for(String name : names) {
      try {
        uris.add(stringAsURI(name));
      } catch (IOException e) {
        LOG.error("Error while processing URI: " + name, e);
      }
    }
    return uris;
  }
}
