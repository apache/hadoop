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

import java.util.*;
import java.io.*;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.util.StringUtils;

/**
 * This class is used in Namesystem's jetty to retrieve a file.
 * Typically used by the Secondary NameNode to retrieve image and
 * edit file for periodic checkpointing.
 */
public class GetImageServlet extends HttpServlet {
  private static final long serialVersionUID = -7669068179452648952L;

  @SuppressWarnings("unchecked")
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response
                    ) throws ServletException, IOException {
    Map<String,String[]> pmap = request.getParameterMap();
    try {
      ServletContext context = getServletContext();
      FSImage nnImage = (FSImage)context.getAttribute("name.system.image");
      TransferFsImage ff = new TransferFsImage(pmap, request, response);
      if (ff.getImage()) {
        // send fsImage
        TransferFsImage.getFileServer(response.getOutputStream(),
                                      nnImage.getFsImageName()); 
      } else if (ff.getEdit()) {
        // send edits
        TransferFsImage.getFileServer(response.getOutputStream(),
                                      nnImage.getFsEditName());
      } else if (ff.putImage()) {
        // issue a HTTP get request to download the new fsimage 
        nnImage.validateCheckpointUpload(ff.getToken());
        TransferFsImage.getFileClient(ff.getInfoServer(), "getimage=1", 
                                      nnImage.getFsImageNameCheckpoint());
        nnImage.checkpointUploadDone();
      }
    } catch (Exception ie) {
      String errMsg = "GetImage failed. " + StringUtils.stringifyException(ie);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }
}
