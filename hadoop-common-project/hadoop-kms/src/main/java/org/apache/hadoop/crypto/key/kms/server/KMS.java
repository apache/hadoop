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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.util.StringUtils;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

/**
 * Class providing the REST bindings, via Jersey, for the KMS.
 */
@Path(KMSRESTConstants.SERVICE_VERSION)
@InterfaceAudience.Private
public class KMS {
  private static final String CREATE_KEY = "CREATE_KEY";
  private static final String DELETE_KEY = "DELETE_KEY";
  private static final String ROLL_NEW_VERSION = "ROLL_NEW_VERSION";
  private static final String GET_KEYS = "GET_KEYS";
  private static final String GET_KEYS_METADATA = "GET_KEYS_METADATA";
  private static final String GET_KEY_VERSION = "GET_KEY_VERSION";
  private static final String GET_CURRENT_KEY = "GET_CURRENT_KEY";
  private static final String GET_KEY_VERSIONS = "GET_KEY_VERSIONS";
  private static final String GET_METADATA = "GET_METADATA";

  private KeyProvider provider;

  public KMS() throws Exception {
    provider = KMSWebApp.getKeyProvider();
  }

  private static Principal getPrincipal(SecurityContext securityContext)
      throws AuthenticationException{
    Principal user = securityContext.getUserPrincipal();
    if (user == null) {
      throw new AuthenticationException("User must be authenticated");
    }
    return user;
  }

  private static void assertAccess(KMSACLs.Type aclType, Principal principal,
      String operation, String key) throws AccessControlException {
    if (!KMSWebApp.getACLs().hasAccess(aclType, principal.getName())) {
      KMSWebApp.getUnauthorizedCallsMeter().mark();
      KMSAudit.unauthorized(principal, operation, key);
      throw new AuthorizationException(MessageFormat.format(
          "User:{0} not allowed to do ''{1}'' on ''{2}''",
          principal.getName(), operation, key));
    }
  }

  private static KeyProvider.KeyVersion removeKeyMaterial(
      KeyProvider.KeyVersion keyVersion) {
    return new KMSClientProvider.KMSKeyVersion(keyVersion.getVersionName(),
        null);
  }

  private static URI getKeyURI(String name) throws URISyntaxException {
    return new URI(KMSRESTConstants.SERVICE_VERSION + "/" +
        KMSRESTConstants.KEY_RESOURCE + "/" + name);
  }

  @POST
  @Path(KMSRESTConstants.KEYS_RESOURCE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createKey(@Context SecurityContext securityContext,
      Map jsonKey) throws Exception {
    KMSWebApp.getAdminCallsMeter().mark();
    Principal user = getPrincipal(securityContext);
    String name = (String) jsonKey.get(KMSRESTConstants.NAME_FIELD);
    KMSClientProvider.checkNotEmpty(name, KMSRESTConstants.NAME_FIELD);
    assertAccess(KMSACLs.Type.CREATE, user, CREATE_KEY, name);
    String cipher = (String) jsonKey.get(KMSRESTConstants.CIPHER_FIELD);
    String material = (String) jsonKey.get(KMSRESTConstants.MATERIAL_FIELD);
    int length = (jsonKey.containsKey(KMSRESTConstants.LENGTH_FIELD))
                 ? (Integer) jsonKey.get(KMSRESTConstants.LENGTH_FIELD) : 0;
    String description = (String)
        jsonKey.get(KMSRESTConstants.DESCRIPTION_FIELD);

    if (material != null) {
      assertAccess(KMSACLs.Type.SET_KEY_MATERIAL, user,
          CREATE_KEY + " with user provided material", name);
    }
    KeyProvider.Options options = new KeyProvider.Options(
        KMSWebApp.getConfiguration());
    if (cipher != null) {
      options.setCipher(cipher);
    }
    if (length != 0) {
      options.setBitLength(length);
    }
    options.setDescription(description);

    KeyProvider.KeyVersion keyVersion = (material != null)
        ? provider.createKey(name, Base64.decodeBase64(material), options)
        : provider.createKey(name, options);

    provider.flush();

    KMSAudit.ok(user, CREATE_KEY, name, "UserProvidedMaterial:" +
        (material != null) + " Description:" + description);

    if (!KMSWebApp.getACLs().hasAccess(KMSACLs.Type.GET, user.getName())) {
      keyVersion = removeKeyMaterial(keyVersion);
    }
    Map json = KMSServerJSONUtils.toJSON(keyVersion);
    String requestURL = KMSMDCFilter.getURL();
    int idx = requestURL.lastIndexOf(KMSRESTConstants.KEYS_RESOURCE);
    requestURL = requestURL.substring(0, idx);
    String keyURL = requestURL + KMSRESTConstants.KEY_RESOURCE + "/" + name;
    return Response.created(getKeyURI(name)).type(MediaType.APPLICATION_JSON).
        header("Location", keyURL).entity(json).build();
  }

  @DELETE
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}")
  public Response deleteKey(@Context SecurityContext securityContext,
      @PathParam("name") String name) throws Exception {
    KMSWebApp.getAdminCallsMeter().mark();
    Principal user = getPrincipal(securityContext);
    assertAccess(KMSACLs.Type.DELETE, user, DELETE_KEY, name);
    KMSClientProvider.checkNotEmpty(name, "name");
    provider.deleteKey(name);
    provider.flush();

    KMSAudit.ok(user, DELETE_KEY, name, "");

    return Response.ok().build();
  }

  @POST
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response rolloverKey(@Context SecurityContext securityContext,
      @PathParam("name") String name, Map jsonMaterial)
      throws Exception {
    KMSWebApp.getAdminCallsMeter().mark();
    Principal user = getPrincipal(securityContext);
    assertAccess(KMSACLs.Type.ROLLOVER, user, ROLL_NEW_VERSION, name);
    KMSClientProvider.checkNotEmpty(name, "name");
    String material = (String)
        jsonMaterial.get(KMSRESTConstants.MATERIAL_FIELD);
    if (material != null) {
      assertAccess(KMSACLs.Type.SET_KEY_MATERIAL, user,
          ROLL_NEW_VERSION + " with user provided material", name);
    }
    KeyProvider.KeyVersion keyVersion = (material != null)
        ? provider.rollNewVersion(name, Base64.decodeBase64(material))
        : provider.rollNewVersion(name);

    provider.flush();

    KMSAudit.ok(user, ROLL_NEW_VERSION, name, "UserProvidedMaterial:" +
        (material != null) + " NewVersion:" + keyVersion.getVersionName());

    if (!KMSWebApp.getACLs().hasAccess(KMSACLs.Type.GET, user.getName())) {
      keyVersion = removeKeyMaterial(keyVersion);
    }
    Map json = KMSServerJSONUtils.toJSON(keyVersion);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(json).build();
  }

  @GET
  @Path(KMSRESTConstants.KEYS_METADATA_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeysMetadata(@Context SecurityContext securityContext,
      @QueryParam(KMSRESTConstants.KEY_OP) List<String> keyNamesList)
      throws Exception {
    KMSWebApp.getAdminCallsMeter().mark();
    Principal user = getPrincipal(securityContext);
    String[] keyNames = keyNamesList.toArray(new String[keyNamesList.size()]);
    String names = StringUtils.arrayToString(keyNames);
    assertAccess(KMSACLs.Type.GET_METADATA, user, GET_KEYS_METADATA, names);
    KeyProvider.Metadata[] keysMeta = provider.getKeysMetadata(keyNames);
    Object json = KMSServerJSONUtils.toJSON(keyNames, keysMeta);
    KMSAudit.ok(user, GET_KEYS_METADATA, names, "");
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(json).build();
  }

  @GET
  @Path(KMSRESTConstants.KEYS_NAMES_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeyNames(@Context SecurityContext securityContext)
      throws Exception {
    KMSWebApp.getAdminCallsMeter().mark();
    Principal user = getPrincipal(securityContext);
    assertAccess(KMSACLs.Type.GET_KEYS, user, GET_KEYS, "*");
    Object json = provider.getKeys();
    KMSAudit.ok(user, GET_KEYS, "*", "");
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(json).build();
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}")
  public Response getKey(@Context SecurityContext securityContext,
      @PathParam("name") String name)
      throws Exception {
    return getMetadata(securityContext, name);
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.METADATA_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMetadata(@Context SecurityContext securityContext,
      @PathParam("name") String name)
      throws Exception {
    Principal user = getPrincipal(securityContext);
    KMSClientProvider.checkNotEmpty(name, "name");
    KMSWebApp.getAdminCallsMeter().mark();
    assertAccess(KMSACLs.Type.GET_METADATA, user, GET_METADATA, name);
    Object json = KMSServerJSONUtils.toJSON(name, provider.getMetadata(name));
    KMSAudit.ok(user, GET_METADATA, name, "");
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(json).build();
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.CURRENT_VERSION_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCurrentVersion(@Context SecurityContext securityContext,
      @PathParam("name") String name)
      throws Exception {
    Principal user = getPrincipal(securityContext);
    KMSClientProvider.checkNotEmpty(name, "name");
    KMSWebApp.getKeyCallsMeter().mark();
    assertAccess(KMSACLs.Type.GET, user, GET_CURRENT_KEY, name);
    Object json = KMSServerJSONUtils.toJSON(provider.getCurrentKey(name));
    KMSAudit.ok(user, GET_CURRENT_KEY, name, "");
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(json).build();
  }

  @GET
  @Path(KMSRESTConstants.KEY_VERSION_RESOURCE + "/{versionName:.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeyVersion(@Context SecurityContext securityContext,
      @PathParam("versionName") String versionName)
      throws Exception {
    Principal user = getPrincipal(securityContext);
    KMSClientProvider.checkNotEmpty(versionName, "versionName");
    KMSWebApp.getKeyCallsMeter().mark();
    assertAccess(KMSACLs.Type.GET, user, GET_KEY_VERSION, versionName);
    Object json = KMSServerJSONUtils.toJSON(provider.getKeyVersion(versionName));
    KMSAudit.ok(user, GET_KEY_VERSION, versionName, "");
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(json).build();
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.VERSIONS_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeyVersions(@Context SecurityContext securityContext,
      @PathParam("name") String name)
      throws Exception {
    Principal user = getPrincipal(securityContext);
    KMSClientProvider.checkNotEmpty(name, "name");
    KMSWebApp.getKeyCallsMeter().mark();
    assertAccess(KMSACLs.Type.GET, user, GET_KEY_VERSIONS, name);
    Object json = KMSServerJSONUtils.toJSON(provider.getKeyVersions(name));
    KMSAudit.ok(user, GET_KEY_VERSIONS, name, "");
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(json).build();
  }

}
