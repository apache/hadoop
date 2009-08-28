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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.VersionInfo;

import org.znerd.xmlenc.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Obtain meta-information about a filesystem.
 * @see org.apache.hadoop.hdfs.HftpFileSystem
 */
public class ListPathsServlet extends DfsServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  public static final ThreadLocal<SimpleDateFormat> df =
    new ThreadLocal<SimpleDateFormat>() {
      protected SimpleDateFormat initialValue() {
        return HftpFileSystem.getDateFormat();
      }
    };

  /**
   * Write a node to output.
   * Node information includes path, modification, permission, owner and group.
   * For files, it also includes size, replication and block-size. 
   */
  static void writeInfo(FileStatus i, XMLOutputter doc) throws IOException {
    final SimpleDateFormat ldf = df.get();
    doc.startTag(i.isDir() ? "directory" : "file");
    doc.attribute("path", i.getPath().toUri().getPath());
    doc.attribute("modified", ldf.format(new Date(i.getModificationTime())));
    doc.attribute("accesstime", ldf.format(new Date(i.getAccessTime())));
    if (!i.isDir()) {
      doc.attribute("size", String.valueOf(i.getLen()));
      doc.attribute("replication", String.valueOf(i.getReplication()));
      doc.attribute("blocksize", String.valueOf(i.getBlockSize()));
    }
    doc.attribute("permission", (i.isDir()? "d": "-") + i.getPermission());
    doc.attribute("owner", i.getOwner());
    doc.attribute("group", i.getGroup());
    doc.endTag();
  }

  /**
   * Build a map from the query string, setting values and defaults.
   */
  protected Map<String,String> buildRoot(HttpServletRequest request,
      XMLOutputter doc) {
    final String path = request.getPathInfo() != null
      ? request.getPathInfo() : "/";
    final String exclude = request.getParameter("exclude") != null
      ? request.getParameter("exclude") : "\\..*\\.crc";
    final String filter = request.getParameter("filter") != null
      ? request.getParameter("filter") : ".*";
    final boolean recur = request.getParameter("recursive") != null
      && "yes".equals(request.getParameter("recursive"));

    Map<String, String> root = new HashMap<String, String>();
    root.put("path", path);
    root.put("recursive", recur ? "yes" : "no");
    root.put("filter", filter);
    root.put("exclude", exclude);
    root.put("time", df.get().format(new Date()));
    root.put("version", VersionInfo.getVersion());
    return root;
  }

  /**
   * Service a GET request as described below.
   * Request:
   * {@code
   * GET http://<nn>:<port>/listPaths[/<path>][<?option>[&option]*] HTTP/1.1
   * }
   *
   * Where <i>option</i> (default) in:
   * recursive (&quot;no&quot;)
   * filter (&quot;.*&quot;)
   * exclude (&quot;\..*\.crc&quot;)
   *
   * Response: A flat list of files/directories in the following format:
   * {@code
   *   <listing path="..." recursive="(yes|no)" filter="..."
   *            time="yyyy-MM-dd hh:mm:ss UTC" version="...">
   *     <directory path="..." modified="yyyy-MM-dd hh:mm:ss"/>
   *     <file path="..." modified="yyyy-MM-dd'T'hh:mm:ssZ" accesstime="yyyy-MM-dd'T'hh:mm:ssZ" 
   *           blocksize="..."
   *           replication="..." size="..."/>
   *   </listing>
   * }
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final UnixUserGroupInformation ugi = getUGI(request);
    final PrintWriter out = response.getWriter();
    final XMLOutputter doc = new XMLOutputter(out, "UTF-8");
    try {
      final Map<String, String> root = buildRoot(request, doc);
      final String path = root.get("path");
      final boolean recur = "yes".equals(root.get("recursive"));
      final Pattern filter = Pattern.compile(root.get("filter"));
      final Pattern exclude = Pattern.compile(root.get("exclude"));
      ClientProtocol nnproxy = createNameNodeProxy(ugi);

      doc.declaration();
      doc.startTag("listing");
      for (Map.Entry<String,String> m : root.entrySet()) {
        doc.attribute(m.getKey(), m.getValue());
      }

      FileStatus base = nnproxy.getFileInfo(path);
      if ((base != null) && base.isDir()) {
        writeInfo(base, doc);
      }

      Stack<String> pathstack = new Stack<String>();
      pathstack.push(path);
      while (!pathstack.empty()) {
        String p = pathstack.pop();
        try {
          for (FileStatus i : nnproxy.getListing(p)) {
            if (exclude.matcher(i.getPath().getName()).matches()
                || !filter.matcher(i.getPath().getName()).matches()) {
              continue;
            }
            if (recur && i.isDir()) {
              pathstack.push(i.getPath().toUri().getPath());
            }
            writeInfo(i, doc);
          }
        }
        catch(RemoteException re) {re.writeXml(p, doc);}
      }
      if (doc != null) {
        doc.endDocument();
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}
