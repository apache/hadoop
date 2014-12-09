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
package org.apache.hadoop.security.authentication.examples;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;

/**
 * Example that uses <code>AuthenticatedURL</code>.
 */
public class WhoClient {

  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        System.err.println("Usage: <URL>");
        System.exit(-1);
      }
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      URL url = new URL(args[0]);
      HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
      System.out.println();
      System.out.println("Token value: " + token);
      System.out.println("Status code: " + conn.getResponseCode() + " " + conn.getResponseMessage());
      System.out.println();
      if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                conn.getInputStream(), Charset.forName("UTF-8")));
        String line = reader.readLine();
        while (line != null) {
          System.out.println(line);
          line = reader.readLine();
        }
        reader.close();
      }
      System.out.println();
    }
    catch (Exception ex) {
      System.err.println("ERROR: " + ex.getMessage());
      System.exit(-1);
    }
  }
}
