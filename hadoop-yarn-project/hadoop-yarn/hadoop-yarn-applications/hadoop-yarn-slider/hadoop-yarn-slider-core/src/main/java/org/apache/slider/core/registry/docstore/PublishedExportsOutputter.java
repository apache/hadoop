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

package org.apache.slider.core.registry.docstore;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Output a published configuration */
public abstract class PublishedExportsOutputter {

  protected final PublishedExports exports;

  protected PublishedExportsOutputter(PublishedExports exports) {
    this.exports = exports;
  }

  /**
   * Create an outputter for the chosen format
   *
   * @param format  format enumeration
   * @param exports owning config
   * @return the outputter
   */

  public static PublishedExportsOutputter createOutputter(ConfigFormat format,
                                                         PublishedExports exports) {
    Preconditions.checkNotNull(exports);
    switch (format) {
      case JSON:
        return new JsonOutputter(exports);
      default:
        throw new RuntimeException("Unsupported format :" + format);
    }
  }

  public void save(File dest) throws IOException {
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(dest);
      save(out);
      out.close();
    } finally {
      org.apache.hadoop.io.IOUtils.closeStream(out);
    }
  }

  /**
   * Save the content. The default saves the asString() value to the output stream
   *
   * @param out output stream
   * @throws IOException
   */
  public void save(OutputStream out) throws IOException {
    IOUtils.write(asString(), out, Charsets.UTF_8);
  }

  /**
   * Convert to a string
   *
   * @return the string form
   * @throws IOException
   */
  public abstract String asString() throws IOException;

  public static class JsonOutputter extends PublishedExportsOutputter {

    public JsonOutputter(PublishedExports exports) {
      super(exports);
    }

    @Override
    public void save(File dest) throws IOException {
      FileUtils.writeStringToFile(dest, asString(), Charsets.UTF_8);
    }

    @Override
    public String asString() throws IOException {
      return exports.asJson();
    }
  }
}
