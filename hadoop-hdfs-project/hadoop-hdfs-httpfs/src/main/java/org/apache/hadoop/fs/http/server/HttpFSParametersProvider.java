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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation;
import org.apache.hadoop.lib.wsrs.BooleanParam;
import org.apache.hadoop.lib.wsrs.EnumParam;
import org.apache.hadoop.lib.wsrs.EnumSetParam;
import org.apache.hadoop.lib.wsrs.LongParam;
import org.apache.hadoop.lib.wsrs.Param;
import org.apache.hadoop.lib.wsrs.ParametersProvider;
import org.apache.hadoop.lib.wsrs.ShortParam;
import org.apache.hadoop.lib.wsrs.StringParam;

import javax.ws.rs.ext.Provider;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;

/**
 * HttpFS ParametersProvider.
 */
@Provider
@InterfaceAudience.Private
@SuppressWarnings("unchecked")
public class HttpFSParametersProvider extends ParametersProvider {

  private static final Map<Enum, Class<Param<?>>[]> PARAMS_DEF =
    new HashMap<Enum, Class<Param<?>>[]>();

  static {
    PARAMS_DEF.put(Operation.OPEN,
        new Class[]{OffsetParam.class, LenParam.class});
    PARAMS_DEF.put(Operation.GETFILESTATUS, new Class[]{});
    PARAMS_DEF.put(Operation.LISTSTATUS, new Class[]{FilterParam.class});
    PARAMS_DEF.put(Operation.GETHOMEDIRECTORY, new Class[]{});
    PARAMS_DEF.put(Operation.GETCONTENTSUMMARY, new Class[]{});
    PARAMS_DEF.put(Operation.GETFILECHECKSUM, new Class[]{});
    PARAMS_DEF.put(Operation.GETFILEBLOCKLOCATIONS, new Class[]{});
    PARAMS_DEF.put(Operation.GETACLSTATUS, new Class[]{});
    PARAMS_DEF.put(Operation.INSTRUMENTATION, new Class[]{});
    PARAMS_DEF.put(Operation.APPEND, new Class[]{DataParam.class});
    PARAMS_DEF.put(Operation.CONCAT, new Class[]{SourcesParam.class});
    PARAMS_DEF.put(Operation.CREATE,
      new Class[]{PermissionParam.class, OverwriteParam.class,
                  ReplicationParam.class, BlockSizeParam.class, DataParam.class});
    PARAMS_DEF.put(Operation.MKDIRS, new Class[]{PermissionParam.class});
    PARAMS_DEF.put(Operation.RENAME, new Class[]{DestinationParam.class});
    PARAMS_DEF.put(Operation.SETOWNER,
        new Class[]{OwnerParam.class, GroupParam.class});
    PARAMS_DEF.put(Operation.SETPERMISSION, new Class[]{PermissionParam.class});
    PARAMS_DEF.put(Operation.SETREPLICATION,
        new Class[]{ReplicationParam.class});
    PARAMS_DEF.put(Operation.SETTIMES,
        new Class[]{ModifiedTimeParam.class, AccessTimeParam.class});
    PARAMS_DEF.put(Operation.DELETE, new Class[]{RecursiveParam.class});
    PARAMS_DEF.put(Operation.SETACL, new Class[]{AclPermissionParam.class});
    PARAMS_DEF.put(Operation.REMOVEACL, new Class[]{});
    PARAMS_DEF.put(Operation.MODIFYACLENTRIES,
        new Class[]{AclPermissionParam.class});
    PARAMS_DEF.put(Operation.REMOVEACLENTRIES,
        new Class[]{AclPermissionParam.class});
    PARAMS_DEF.put(Operation.REMOVEDEFAULTACL, new Class[]{});
    PARAMS_DEF.put(Operation.SETXATTR,
        new Class[]{XAttrNameParam.class, XAttrValueParam.class,
                  XAttrSetFlagParam.class});
    PARAMS_DEF.put(Operation.REMOVEXATTR, new Class[]{XAttrNameParam.class});
    PARAMS_DEF.put(Operation.GETXATTRS, 
        new Class[]{XAttrNameParam.class, XAttrEncodingParam.class});
    PARAMS_DEF.put(Operation.LISTXATTRS, new Class[]{});
  }

  public HttpFSParametersProvider() {
    super(HttpFSFileSystem.OP_PARAM, HttpFSFileSystem.Operation.class,
          PARAMS_DEF);
  }

  /**
   * Class for access-time parameter.
   */
  @InterfaceAudience.Private
  public static class AccessTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.ACCESS_TIME_PARAM;
    /**
     * Constructor.
     */
    public AccessTimeParam() {
      super(NAME, -1l);
    }
  }

  /**
   * Class for block-size parameter.
   */
  @InterfaceAudience.Private
  public static class BlockSizeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.BLOCKSIZE_PARAM;

    /**
     * Constructor.
     */
    public BlockSizeParam() {
      super(NAME, -1l);
    }
  }

  /**
   * Class for data parameter.
   */
  @InterfaceAudience.Private
  public static class DataParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "data";

    /**
     * Constructor.
     */
    public DataParam() {
      super(NAME, false);
    }
  }

  /**
   * Class for operation parameter.
   */
  @InterfaceAudience.Private
  public static class OperationParam extends EnumParam<HttpFSFileSystem.Operation> {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OP_PARAM;
    /**
     * Constructor.
     */
    public OperationParam(String operation) {
      super(NAME, HttpFSFileSystem.Operation.class,
            HttpFSFileSystem.Operation.valueOf(operation.toUpperCase()));
    }
  }

  /**
   * Class for delete's recursive parameter.
   */
  @InterfaceAudience.Private
  public static class RecursiveParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.RECURSIVE_PARAM;

    /**
     * Constructor.
     */
    public RecursiveParam() {
      super(NAME, false);
    }
  }

  /**
   * Class for filter parameter.
   */
  @InterfaceAudience.Private
  public static class FilterParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "filter";

    /**
     * Constructor.
     */
    public FilterParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for group parameter.
   */
  @InterfaceAudience.Private
  public static class GroupParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.GROUP_PARAM;

    /**
     * Constructor.
     */
    public GroupParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for len parameter.
   */
  @InterfaceAudience.Private
  public static class LenParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "length";

    /**
     * Constructor.
     */
    public LenParam() {
      super(NAME, -1l);
    }
  }

  /**
   * Class for modified-time parameter.
   */
  @InterfaceAudience.Private
  public static class ModifiedTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.MODIFICATION_TIME_PARAM;

    /**
     * Constructor.
     */
    public ModifiedTimeParam() {
      super(NAME, -1l);
    }
  }

  /**
   * Class for offset parameter.
   */
  @InterfaceAudience.Private
  public static class OffsetParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "offset";

    /**
     * Constructor.
     */
    public OffsetParam() {
      super(NAME, 0l);
    }
  }

  /**
   * Class for overwrite parameter.
   */
  @InterfaceAudience.Private
  public static class OverwriteParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OVERWRITE_PARAM;

    /**
     * Constructor.
     */
    public OverwriteParam() {
      super(NAME, true);
    }
  }

  /**
   * Class for owner parameter.
   */
  @InterfaceAudience.Private
  public static class OwnerParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OWNER_PARAM;

    /**
     * Constructor.
     */
    public OwnerParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for permission parameter.
   */
  @InterfaceAudience.Private
  public static class PermissionParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.PERMISSION_PARAM;


    /**
     * Constructor.
     */
    public PermissionParam() {
      super(NAME, HttpFSFileSystem.DEFAULT_PERMISSION, 8);
    }

  }

  /**
   * Class for AclPermission parameter.
   */
  @InterfaceAudience.Private
  public static class AclPermissionParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.ACLSPEC_PARAM;

    /**
     * Constructor.
     */
    public AclPermissionParam() {
      super(NAME, HttpFSFileSystem.ACLSPEC_DEFAULT,
              Pattern.compile(DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));
    }
  }

  /**
   * Class for replication parameter.
   */
  @InterfaceAudience.Private
  public static class ReplicationParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.REPLICATION_PARAM;

    /**
     * Constructor.
     */
    public ReplicationParam() {
      super(NAME, (short) -1);
    }
  }

  /**
   * Class for concat sources parameter.
   */
  @InterfaceAudience.Private
  public static class SourcesParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.SOURCES_PARAM;

    /**
     * Constructor.
     */
    public SourcesParam() {
      super(NAME, null);
    }
  }

  /**
   * Class for to-path parameter.
   */
  @InterfaceAudience.Private
  public static class DestinationParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.DESTINATION_PARAM;

    /**
     * Constructor.
     */
    public DestinationParam() {
      super(NAME, null);
    }
  }
  
  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrNameParam extends StringParam {
    public static final String XATTR_NAME_REGX = 
        "^(user\\.|trusted\\.|system\\.|security\\.).+";
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_NAME_PARAM;
    private static final Pattern pattern = Pattern.compile(XATTR_NAME_REGX);

    /**
     * Constructor.
     */
    public XAttrNameParam() {
      super(NAME, null, pattern);
    }
  }

  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrValueParam extends StringParam {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_VALUE_PARAM;

    /**
     * Constructor.
     */
    public XAttrValueParam() {
      super(NAME, null);
    }
  }

  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrSetFlagParam extends EnumSetParam<XAttrSetFlag> {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_SET_FLAG_PARAM;

    /**
     * Constructor.
     */
    public XAttrSetFlagParam() {
      super(NAME, XAttrSetFlag.class, null);
    }
  }

  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrEncodingParam extends EnumParam<XAttrCodec> {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_ENCODING_PARAM;

    /**
     * Constructor.
     */
    public XAttrEncodingParam() {
      super(NAME, XAttrCodec.class, null);
    }
  }
}
