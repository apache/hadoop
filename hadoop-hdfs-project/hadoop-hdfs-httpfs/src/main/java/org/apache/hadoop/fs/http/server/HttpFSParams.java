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
package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.lib.wsrs.BooleanParam;
import org.apache.hadoop.lib.wsrs.EnumParam;
import org.apache.hadoop.lib.wsrs.LongParam;
import org.apache.hadoop.lib.wsrs.ShortParam;
import org.apache.hadoop.lib.wsrs.StringParam;
import org.apache.hadoop.lib.wsrs.UserProvider;
import org.slf4j.MDC;

import java.util.regex.Pattern;

/**
 * HttpFS HTTP Parameters used by {@link HttpFSServer}.
 */
public class HttpFSParams {

  /**
   * To avoid instantiation.
   */
  private HttpFSParams() {
  }

  /**
   * Class for access-time parameter.
   */
  public static class AccessTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.ACCESS_TIME_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "-1";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public AccessTimeParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for block-size parameter.
   */
  public static class BlockSizeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.BLOCKSIZE_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "-1";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public BlockSizeParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for data parameter.
   */
  public static class DataParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "data";

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "false";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public DataParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for DELETE operation parameter.
   */
  public static class DeleteOpParam extends EnumParam<HttpFSFileSystem.DeleteOpValues> {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OP_PARAM;

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public DeleteOpParam(String str) {
      super(NAME, str, HttpFSFileSystem.DeleteOpValues.class);
    }
  }

  /**
   * Class for delete's recursive parameter.
   */
  public static class DeleteRecursiveParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.RECURSIVE_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "false";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public DeleteRecursiveParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for do-as parameter.
   */
  public static class DoAsParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.DO_AS_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public DoAsParam(String str) {
      super(NAME, str, UserProvider.USER_PATTERN);
    }

    /**
     * Delegates to parent and then adds do-as user to
     * MDC context for logging purposes.
     *
     * @param name parameter name.
     * @param str parameter value.
     *
     * @return parsed parameter
     */
    @Override
    public String parseParam(String name, String str) {
      String doAs = super.parseParam(name, str);
      MDC.put(NAME, (doAs != null) ? doAs : "-");
      return doAs;
    }
  }

  /**
   * Class for filter parameter.
   */
  public static class FilterParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "filter";

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "";

    /**
     * Constructor.
     *
     * @param expr parameter value.
     */
    public FilterParam(String expr) {
      super(NAME, expr);
    }

  }

  /**
   * Class for path parameter.
   */
  public static class FsPathParam extends StringParam {

    /**
     * Constructor.
     *
     * @param path parameter value.
     */
    public FsPathParam(String path) {
      super("path", path);
    }

    /**
     * Makes the path absolute adding '/' to it.
     * <p/>
     * This is required because JAX-RS resolution of paths does not add
     * the root '/'.
     */
    public void makeAbsolute() {
      String path = value();
      path = "/" + ((path != null) ? path : "");
      setValue(path);
    }

  }

  /**
   * Class for GET operation parameter.
   */
  public static class GetOpParam extends EnumParam<HttpFSFileSystem.GetOpValues> {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OP_PARAM;

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public GetOpParam(String str) {
      super(NAME, str, HttpFSFileSystem.GetOpValues.class);
    }
  }

  /**
   * Class for group parameter.
   */
  public static class GroupParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.GROUP_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public GroupParam(String str) {
      super(NAME, str, UserProvider.USER_PATTERN);
    }

  }

  /**
   * Class for len parameter.
   */
  public static class LenParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "len";

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "-1";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public LenParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for modified-time parameter.
   */
  public static class ModifiedTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.MODIFICATION_TIME_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "-1";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public ModifiedTimeParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for offset parameter.
   */
  public static class OffsetParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "offset";

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "0";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public OffsetParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for overwrite parameter.
   */
  public static class OverwriteParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OVERWRITE_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "true";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public OverwriteParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for owner parameter.
   */
  public static class OwnerParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OWNER_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public OwnerParam(String str) {
      super(NAME, str, UserProvider.USER_PATTERN);
    }

  }

  /**
   * Class for permission parameter.
   */
  public static class PermissionParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.PERMISSION_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = HttpFSFileSystem.DEFAULT_PERMISSION;


    /**
     * Symbolic Unix permissions regular expression pattern.
     */
    private static final Pattern PERMISSION_PATTERN =
      Pattern.compile(DEFAULT + "|(-[-r][-w][-x][-r][-w][-x][-r][-w][-x])" + "|[0-7][0-7][0-7]");

    /**
     * Constructor.
     *
     * @param permission parameter value.
     */
    public PermissionParam(String permission) {
      super(NAME, permission.toLowerCase(), PERMISSION_PATTERN);
    }

  }

  /**
   * Class for POST operation parameter.
   */
  public static class PostOpParam extends EnumParam<HttpFSFileSystem.PostOpValues> {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OP_PARAM;

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public PostOpParam(String str) {
      super(NAME, str, HttpFSFileSystem.PostOpValues.class);
    }
  }

  /**
   * Class for PUT operation parameter.
   */
  public static class PutOpParam extends EnumParam<HttpFSFileSystem.PutOpValues> {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OP_PARAM;

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public PutOpParam(String str) {
      super(NAME, str, HttpFSFileSystem.PutOpValues.class);
    }
  }

  /**
   * Class for replication parameter.
   */
  public static class ReplicationParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.REPLICATION_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "-1";

    /**
     * Constructor.
     *
     * @param str parameter value.
     */
    public ReplicationParam(String str) {
      super(NAME, str);
    }
  }

  /**
   * Class for to-path parameter.
   */
  public static class ToPathParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.DESTINATION_PARAM;

    /**
     * Default parameter value.
     */
    public static final String DEFAULT = "";

    /**
     * Constructor.
     *
     * @param path parameter value.
     */
    public ToPathParam(String path) {
      super(NAME, path);
    }
  }
}
