/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.jmx;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For example in case of MutableGauge we are using numbers,
 * but not implementing Number interface,
 * so we skip class check here because we can not be sure NaN values are wrapped
 * with classes which implements the Number interface
 */
public class JMXJsonServletNaNFiltered extends JMXJsonServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(JMXJsonServletNaNFiltered.class);

  @Override
  protected boolean extraCheck(Object value) {
    return Objects.equals("NaN", Objects.toString(value).trim());
  }

  @Override
  protected void extraWrite(Object value, String attName, JsonGenerator jg) throws IOException {
    LOG.debug("The {} attribute with value: {} was identified as NaN "
        + "and will be replaced with 0.0", attName, value);
    jg.writeNumber(0.0);
  }
}
