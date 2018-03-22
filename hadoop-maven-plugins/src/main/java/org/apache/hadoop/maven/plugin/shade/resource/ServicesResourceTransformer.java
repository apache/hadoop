/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.maven.plugin.shade.resource;

import java.io.BufferedReader;
import org.apache.maven.plugins.shade.relocation.Relocator;
import org.apache.maven.plugins.shade.resource.ResourceTransformer;
import org.codehaus.plexus.util.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Resources transformer that appends entries in META-INF/services resources
 * into a single resource. For example, if there are several
 * META-INF/services/org.apache.maven.project.ProjectBuilder resources spread
 * across many JARs the individual entries will all be concatenated into a
 * single META-INF/services/org.apache.maven.project.ProjectBuilder resource
 * packaged into the resultant JAR produced by the shading process.
 *
 * From following sources, only needed until MSHADE-182 gets released
 * * https://s.apache.org/vwjl (source in maven-shade-plugin repo)
 * * https://issues.apache.org/jira/secure/attachment/12718938/MSHADE-182.patch
 *
 * Has been reformatted according to Hadoop checkstyle rules and modified
 * to meet Hadoop's threshold for Findbugs problems.
 */
public class ServicesResourceTransformer
    implements ResourceTransformer {

  private static final String SERVICES_PATH = "META-INF/services";

  private Map<String, ServiceStream> serviceEntries = new HashMap<>();

  private List<Relocator> relocators;

  public boolean canTransformResource(String resource) {
    if (resource.startsWith(SERVICES_PATH)) {
      return true;
    }

    return false;
  }

  public void processResource(String resource, InputStream is,
      List<Relocator> relocatorz) throws IOException {
    ServiceStream out = serviceEntries.get(resource);
    if (out == null) {
      out = new ServiceStream();
      serviceEntries.put(resource, out);
    }

    out.append(is);
    is.close();

    if (this.relocators == null) {
      this.relocators = relocatorz;
    }
  }

  public boolean hasTransformedResource() {
    return serviceEntries.size() > 0;
  }

  public void modifyOutputStream(JarOutputStream jos)
      throws IOException {
    for (Map.Entry<String, ServiceStream> entry : serviceEntries.entrySet()) {
      String key = entry.getKey();
      ServiceStream data = entry.getValue();

      if (relocators != null) {
        key = key.substring(SERVICES_PATH.length() + 1);
        for (Relocator relocator : relocators) {
          if (relocator.canRelocateClass(key)) {
            key = relocator.relocateClass(key);
            break;
          }
        }

        key = SERVICES_PATH + '/' + key;
      }

      jos.putNextEntry(new JarEntry(key));

      //read the content of service file for candidate classes for relocation
      //presume everything is UTF8, because Findbugs barfs on default
      //charset and this seems no worse a choice ¯\_(ツ)_/¯
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(jos,
          StandardCharsets.UTF_8));
      InputStreamReader streamReader =
          new InputStreamReader(data.toInputStream(), StandardCharsets.UTF_8);
      BufferedReader reader = new BufferedReader(streamReader);
      String className;

      while ((className = reader.readLine()) != null) {

        if (relocators != null) {
          for (Relocator relocator : relocators) {
            //if the class can be relocated then relocate it
            if (relocator.canRelocateClass(className)) {
              className = relocator.applyToSourceContent(className);
              break;
            }
          }
        }

        writer.println(className);
        writer.flush();
      }

      reader.close();
      data.reset();
    }
  }

  static class ServiceStream extends ByteArrayOutputStream {

    public ServiceStream() {
      super(1024);
    }

    public void append(InputStream is)
        throws IOException {
      if (count > 0 && buf[count - 1] != '\n' && buf[count - 1] != '\r') {
        write('\n');
      }

      IOUtil.copy(is, this);
    }

    public InputStream toInputStream() {
      return new ByteArrayInputStream(buf, 0, count);
    }

  }

}
