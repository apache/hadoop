/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli.param;

import org.apache.commons.cli.ParseException;

/**
 * A class represents quick links to a web page.
 */
public class Quicklink {
  private String label;
  private String componentInstanceName;
  private String protocol;
  private int port;

  public void parse(String quicklinkStr) throws ParseException {
    if (!quicklinkStr.contains("=")) {
      throw new ParseException("Should be <label>=<link> format for quicklink");
    }

    int index = quicklinkStr.indexOf("=");
    label = quicklinkStr.substring(0, index);
    quicklinkStr = quicklinkStr.substring(index + 1);

    if (quicklinkStr.startsWith("http://")) {
      protocol = "http://";
    } else if (quicklinkStr.startsWith("https://")) {
      protocol = "https://";
    } else {
      throw new ParseException("Quicklink should start with http or https");
    }

    quicklinkStr = quicklinkStr.substring(protocol.length());
    index = quicklinkStr.indexOf(":");

    if (index == -1) {
      throw new ParseException("Quicklink should be componet-id:port form");
    }

    componentInstanceName = quicklinkStr.substring(0, index);
    port = Integer.parseInt(quicklinkStr.substring(index + 1));
  }

  public String getLabel() {
    return label;
  }

  public String getComponentInstanceName() {
    return componentInstanceName;
  }

  public String getProtocol() {
    return protocol;
  }

  public int getPort() {
    return port;
  }
}
