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

import com.google.common.base.Preconditions;
import org.apache.hadoop.util.KMSUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.util.KMSUtil.checkNotEmpty;
import static org.apache.hadoop.util.KMSUtil.checkNotNull;

/**
 * Class providing the REST bindings, via Jersey, for the KMS.
 */
@Path(KMSRESTConstants.SERVICE_VERSION)
@InterfaceAudience.Private
public class KMS {

  public enum KMSOp {
    CREATE_KEY, DELETE_KEY, ROLL_NEW_VERSION, INVALIDATE_CACHE,
    GET_KEYS, GET_KEYS_METADATA,
    GET_KEY_VERSIONS, GET_METADATA, GET_KEY_VERSION, GET_CURRENT_KEY,
    GENERATE_EEK, DECRYPT_EEK, REENCRYPT_EEK, REENCRYPT_EEK_BATCH
  }

  private KeyProviderCryptoExtension provider;
  private KMSAudit kmsAudit;

  static final Logger LOG = LoggerFactory.getLogger(KMS.class);

  private static final int MAX_NUM_PER_BATCH = 10000;

  public KMS() throws Exception {
    provider = KMSWebApp.getKeyProvider();
    kmsAudit= KMSWebApp.getKMSAudit();
  }

  private void assertAccess(KMSACLs.Type aclType, UserGroupInformation ugi,
      KMSOp operation) throws AccessControlException {
    KMSWebApp.getACLs().assertAccess(aclType, ugi, operation, null);
  }

  private void assertAccess(KMSACLs.Type aclType, UserGroupInformation ugi,
      KMSOp operation, String key) throws AccessControlException {
    KMSWebApp.getACLs().assertAccess(aclType, ugi, operation, key);
  }

  private static KeyProvider.KeyVersion removeKeyMaterial(
      KeyProvider.KeyVersion keyVersion) {
    return new KMSClientProvider.KMSKeyVersion(keyVersion.getName(),
        keyVersion.getVersionName(), null);
  }

  private static URI getKeyURI(String domain, String keyName) {
    return UriBuilder.fromPath("{a}/{b}/{c}")
        .build(domain, KMSRESTConstants.KEY_RESOURCE, keyName);
  }

  @POST
  @Path(KMSRESTConstants.KEYS_RESOURCE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  @SuppressWarnings("unchecked")
  public Response createKey(Map jsonKey) throws Exception {
    try{
      LOG.trace("Entering createKey Method.");
      KMSWebApp.getAdminCallsMeter().mark();
      UserGroupInformation user = HttpUserGroupInformation.get();
      final String name = (String) jsonKey.get(KMSRESTConstants.NAME_FIELD);
      checkNotEmpty(name, KMSRESTConstants.NAME_FIELD);
      assertAccess(KMSACLs.Type.CREATE, user, KMSOp.CREATE_KEY, name);
      String cipher = (String) jsonKey.get(KMSRESTConstants.CIPHER_FIELD);
      final String material;
      material = (String) jsonKey.get(KMSRESTConstants.MATERIAL_FIELD);
      int length = (jsonKey.containsKey(KMSRESTConstants.LENGTH_FIELD))
                   ? (Integer) jsonKey.get(KMSRESTConstants.LENGTH_FIELD) : 0;
      String description = (String)
          jsonKey.get(KMSRESTConstants.DESCRIPTION_FIELD);
      LOG.debug("Creating key with name {}, cipher being used{}, " +
              "length of key {}, description of key {}", name, cipher,
               length, description);
      Map<String, String> attributes = (Map<String, String>)
          jsonKey.get(KMSRESTConstants.ATTRIBUTES_FIELD);
      if (material != null) {
        assertAccess(KMSACLs.Type.SET_KEY_MATERIAL, user,
            KMSOp.CREATE_KEY, name);
      }
      final KeyProvider.Options options = new KeyProvider.Options(
          KMSWebApp.getConfiguration());
      if (cipher != null) {
        options.setCipher(cipher);
      }
      if (length != 0) {
        options.setBitLength(length);
      }
      options.setDescription(description);
      options.setAttributes(attributes);

      KeyProvider.KeyVersion keyVersion = user.doAs(
          new PrivilegedExceptionAction<KeyVersion>() {
            @Override
            public KeyVersion run() throws Exception {
              KeyProvider.KeyVersion keyVersion = (material != null)
                  ? provider.createKey(name, Base64.decodeBase64(material),
                      options)
                  : provider.createKey(name, options);
              provider.flush();
              return keyVersion;
            }
          }
      );

      kmsAudit.ok(user, KMSOp.CREATE_KEY, name, "UserProvidedMaterial:" +
          (material != null) + " Description:" + description);

      if (!KMSWebApp.getACLs().hasAccess(KMSACLs.Type.GET, user)) {
        keyVersion = removeKeyMaterial(keyVersion);
      }
      Map json = KMSUtil.toJSON(keyVersion);
      String requestURL = KMSMDCFilter.getURL();
      int idx = requestURL.lastIndexOf(KMSRESTConstants.KEYS_RESOURCE);
      requestURL = requestURL.substring(0, idx);
      LOG.trace("Exiting createKey Method.");
      return Response.created(getKeyURI(KMSRESTConstants.SERVICE_VERSION, name))
          .type(MediaType.APPLICATION_JSON)
          .header("Location", getKeyURI(requestURL, name)).entity(json).build();
    } catch (Exception e) {
      LOG.debug("Exception in createKey.", e);
      throw e;
    }
  }

  @DELETE
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}")
  public Response deleteKey(@PathParam("name") final String name)
      throws Exception {
    try {
      LOG.trace("Entering deleteKey method.");
      KMSWebApp.getAdminCallsMeter().mark();
      UserGroupInformation user = HttpUserGroupInformation.get();
      assertAccess(KMSACLs.Type.DELETE, user, KMSOp.DELETE_KEY, name);
      checkNotEmpty(name, "name");
      LOG.debug("Deleting key with name {}.", name);
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          provider.deleteKey(name);
          provider.flush();
          return null;
        }
      });

      kmsAudit.ok(user, KMSOp.DELETE_KEY, name, "");
      LOG.trace("Exiting deleteKey method.");
      return Response.ok().build();
    } catch (Exception e) {
      LOG.debug("Exception in deleteKey.", e);
      throw e;
    }
  }

  @POST
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response rolloverKey(@PathParam("name") final String name,
      Map jsonMaterial) throws Exception {
    try {
      LOG.trace("Entering rolloverKey Method.");
      KMSWebApp.getAdminCallsMeter().mark();
      UserGroupInformation user = HttpUserGroupInformation.get();
      assertAccess(KMSACLs.Type.ROLLOVER, user, KMSOp.ROLL_NEW_VERSION, name);
      checkNotEmpty(name, "name");
      LOG.debug("Rolling key with name {}.", name);
      final String material = (String)
              jsonMaterial.get(KMSRESTConstants.MATERIAL_FIELD);
      if (material != null) {
        assertAccess(KMSACLs.Type.SET_KEY_MATERIAL, user,
                KMSOp.ROLL_NEW_VERSION, name);
      }

      KeyProvider.KeyVersion keyVersion = user.doAs(
              new PrivilegedExceptionAction<KeyVersion>() {
              @Override
                public KeyVersion run() throws Exception {
                KeyVersion keyVersion = (material != null)
                        ? provider.rollNewVersion(name,
                        Base64.decodeBase64(material))
                        : provider.rollNewVersion(name);
                provider.flush();
                return keyVersion;
              }
            }
      );

      kmsAudit.ok(user, KMSOp.ROLL_NEW_VERSION, name, "UserProvidedMaterial:" +
              (material != null) +
              " NewVersion:" + keyVersion.getVersionName());

      if (!KMSWebApp.getACLs().hasAccess(KMSACLs.Type.GET, user)) {
        keyVersion = removeKeyMaterial(keyVersion);
      }
      Map json = KMSUtil.toJSON(keyVersion);
      LOG.trace("Exiting rolloverKey Method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(json)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in rolloverKey.", e);
      throw e;
    }
  }

  @POST
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/"
      + KMSRESTConstants.INVALIDATECACHE_RESOURCE)
  public Response invalidateCache(@PathParam("name") final String name)
      throws Exception {
    try {
      LOG.trace("Entering invalidateCache Method.");
      KMSWebApp.getAdminCallsMeter().mark();
      checkNotEmpty(name, "name");
      UserGroupInformation user = HttpUserGroupInformation.get();
      assertAccess(KMSACLs.Type.ROLLOVER, user, KMSOp.INVALIDATE_CACHE, name);
      LOG.debug("Invalidating cache with key name {}.", name);

      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          provider.invalidateCache(name);
          provider.flush();
          return null;
        }
      });

      kmsAudit.ok(user, KMSOp.INVALIDATE_CACHE, name, "");
      LOG.trace("Exiting invalidateCache for key name {}.", name);
      return Response.ok().build();
    } catch (Exception e) {
      LOG.debug("Exception in invalidateCache for key name {}.", name, e);
      throw e;
    }
  }

  @GET
  @Path(KMSRESTConstants.KEYS_METADATA_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getKeysMetadata(@QueryParam(KMSRESTConstants.KEY)
      List<String> keyNamesList) throws Exception {
    try {
      LOG.trace("Entering getKeysMetadata method.");
      KMSWebApp.getAdminCallsMeter().mark();
      UserGroupInformation user = HttpUserGroupInformation.get();
      final String[] keyNames = keyNamesList.toArray(
              new String[keyNamesList.size()]);
      assertAccess(KMSACLs.Type.GET_METADATA, user, KMSOp.GET_KEYS_METADATA);

      KeyProvider.Metadata[] keysMeta = user.doAs(
              new PrivilegedExceptionAction<KeyProvider.Metadata[]>() {
              @Override
                public KeyProvider.Metadata[] run() throws Exception {
                return provider.getKeysMetadata(keyNames);
              }
            }
      );

      Object json = KMSServerJSONUtils.toJSON(keyNames, keysMeta);
      kmsAudit.ok(user, KMSOp.GET_KEYS_METADATA, "");
      LOG.trace("Exiting getKeysMetadata method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(json)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in getKeysmetadata.", e);
      throw e;
    }
  }

  @GET
  @Path(KMSRESTConstants.KEYS_NAMES_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getKeyNames() throws Exception {
    try {
      LOG.trace("Entering getKeyNames method.");
      KMSWebApp.getAdminCallsMeter().mark();
      UserGroupInformation user = HttpUserGroupInformation.get();
      assertAccess(KMSACLs.Type.GET_KEYS, user, KMSOp.GET_KEYS);

      List<String> json = user.doAs(
              new PrivilegedExceptionAction<List<String>>() {
              @Override
                public List<String> run() throws Exception {
                return provider.getKeys();
              }
            }
      );

      kmsAudit.ok(user, KMSOp.GET_KEYS, "");
      LOG.trace("Exiting getKeyNames method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(json)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in getkeyNames.", e);
      throw e;
    }
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}")
  public Response getKey(@PathParam("name") String name)
      throws Exception {
    try {
      LOG.trace("Entering getKey method.");
      LOG.debug("Getting key information for key with name {}.", name);
      LOG.trace("Exiting getKey method.");
      return getMetadata(name);
    } catch (Exception e) {
      LOG.debug("Exception in getKey.", e);
      throw e;
    }
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.METADATA_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getMetadata(@PathParam("name") final String name)
      throws Exception {
    try {
      LOG.trace("Entering getMetadata method.");
      UserGroupInformation user = HttpUserGroupInformation.get();
      checkNotEmpty(name, "name");
      KMSWebApp.getAdminCallsMeter().mark();
      assertAccess(KMSACLs.Type.GET_METADATA, user, KMSOp.GET_METADATA, name);
      LOG.debug("Getting metadata for key with name {}.", name);

      KeyProvider.Metadata metadata = user.doAs(
              new PrivilegedExceptionAction<KeyProvider.Metadata>() {
              @Override
                public KeyProvider.Metadata run() throws Exception {
                return provider.getMetadata(name);
              }
            }
      );

      Object json = KMSServerJSONUtils.toJSON(name, metadata);
      kmsAudit.ok(user, KMSOp.GET_METADATA, name, "");
      LOG.trace("Exiting getMetadata method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(json)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in getMetadata.", e);
      throw e;
    }
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.CURRENT_VERSION_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getCurrentVersion(@PathParam("name") final String name)
      throws Exception {
    try {
      LOG.trace("Entering getCurrentVersion method.");
      UserGroupInformation user = HttpUserGroupInformation.get();
      checkNotEmpty(name, "name");
      KMSWebApp.getKeyCallsMeter().mark();
      assertAccess(KMSACLs.Type.GET, user, KMSOp.GET_CURRENT_KEY, name);
      LOG.debug("Getting key version for key with name {}.", name);

      KeyVersion keyVersion = user.doAs(
              new PrivilegedExceptionAction<KeyVersion>() {
              @Override
                public KeyVersion run() throws Exception {
                return provider.getCurrentKey(name);
            }
            }
      );

      Object json = KMSUtil.toJSON(keyVersion);
      kmsAudit.ok(user, KMSOp.GET_CURRENT_KEY, name, "");
      LOG.trace("Exiting getCurrentVersion method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(json)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in getCurrentVersion.", e);
      throw e;
    }
  }

  @GET
  @Path(KMSRESTConstants.KEY_VERSION_RESOURCE + "/{versionName:.*}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getKeyVersion(
      @PathParam("versionName") final String versionName) throws Exception {
    try {
      LOG.trace("Entering getKeyVersion method.");
      UserGroupInformation user = HttpUserGroupInformation.get();
      checkNotEmpty(versionName, "versionName");
      KMSWebApp.getKeyCallsMeter().mark();
      assertAccess(KMSACLs.Type.GET, user, KMSOp.GET_KEY_VERSION);
      LOG.debug("Getting key with version name {}.", versionName);

      KeyVersion keyVersion = user.doAs(
              new PrivilegedExceptionAction<KeyVersion>() {
              @Override
                public KeyVersion run() throws Exception {
                return provider.getKeyVersion(versionName);
              }
            }
      );

      if (keyVersion != null) {
        kmsAudit.ok(user, KMSOp.GET_KEY_VERSION, keyVersion.getName(), "");
      }
      Object json = KMSUtil.toJSON(keyVersion);
      LOG.trace("Exiting getKeyVersion method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(json)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in getKeyVersion.", e);
      throw e;
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.EEK_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response generateEncryptedKeys(
          @PathParam("name") final String name,
          @QueryParam(KMSRESTConstants.EEK_OP) String edekOp,
          @DefaultValue("1")
          @QueryParam(KMSRESTConstants.EEK_NUM_KEYS) final int numKeys)
          throws Exception {
    try {
      LOG.trace("Entering generateEncryptedKeys method.");
      UserGroupInformation user = HttpUserGroupInformation.get();
      checkNotEmpty(name, "name");
      checkNotNull(edekOp, "eekOp");
      LOG.debug("Generating encrypted key with name {}," +
              " the edek Operation is {}.", name, edekOp);

      Object retJSON;
      if (edekOp.equals(KMSRESTConstants.EEK_GENERATE)) {
        LOG.debug("edek Operation is Generate.");
        assertAccess(KMSACLs.Type.GENERATE_EEK, user, KMSOp.GENERATE_EEK, name);

        final List<EncryptedKeyVersion> retEdeks =
                new LinkedList<EncryptedKeyVersion>();
        try {

          user.doAs(
                  new PrivilegedExceptionAction<Void>() {
                  @Override
                    public Void run() throws Exception {
                    LOG.debug("Generated Encrypted key for {} number of " +
                              "keys.", numKeys);
                    for (int i = 0; i < numKeys; i++) {
                      retEdeks.add(provider.generateEncryptedKey(name));
                    }
                    return null;
                  }
                }
          );

        } catch (Exception e) {
          LOG.error("Exception in generateEncryptedKeys:", e);
          throw new IOException(e);
        }
        kmsAudit.ok(user, KMSOp.GENERATE_EEK, name, "");
        retJSON = new ArrayList();
        for (EncryptedKeyVersion edek : retEdeks) {
          ((ArrayList) retJSON).add(KMSUtil.toJSON(edek));
        }
      } else {
        StringBuilder error;
        error = new StringBuilder("IllegalArgumentException Wrong ");
        error.append(KMSRESTConstants.EEK_OP);
        error.append(" value, it must be ");
        error.append(KMSRESTConstants.EEK_GENERATE);
        error.append(" or ");
        error.append(KMSRESTConstants.EEK_DECRYPT);
        LOG.error(error.toString());
        throw new IllegalArgumentException(error.toString());
      }
      KMSWebApp.getGenerateEEKCallsMeter().mark();
      LOG.trace("Exiting generateEncryptedKeys method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(retJSON)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in generateEncryptedKeys.", e);
      throw e;
    }
  }

  @SuppressWarnings("rawtypes")
  @POST
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.REENCRYPT_BATCH_SUB_RESOURCE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response reencryptEncryptedKeys(
      @PathParam("name") final String name,
      final List<Map> jsonPayload)
      throws Exception {
    LOG.trace("Entering reencryptEncryptedKeys method.");
    try {
      final StopWatch sw = new StopWatch().start();
      checkNotEmpty(name, "name");
      checkNotNull(jsonPayload, "jsonPayload");
      final UserGroupInformation user = HttpUserGroupInformation.get();
      KMSWebApp.getReencryptEEKBatchCallsMeter().mark();
      if (jsonPayload.size() > MAX_NUM_PER_BATCH) {
        LOG.warn("Payload size {} too big for reencryptEncryptedKeys from"
            + " user {}.", jsonPayload.size(), user);
      }
      assertAccess(KMSACLs.Type.GENERATE_EEK, user, KMSOp.REENCRYPT_EEK_BATCH,
          name);
      LOG.debug("Batch reencrypting {} Encrypted Keys for key name {}",
          jsonPayload.size(), name);
      final List<EncryptedKeyVersion> ekvs =
          KMSUtil.parseJSONEncKeyVersions(name, jsonPayload);
      Preconditions.checkArgument(ekvs.size() == jsonPayload.size(),
          "EncryptedKey size mismatch after parsing from json");
      for (EncryptedKeyVersion ekv : ekvs) {
        Preconditions.checkArgument(name.equals(ekv.getEncryptionKeyName()),
            "All EncryptedKeys must be under the given key name " + name);
      }

      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          provider.reencryptEncryptedKeys(ekvs);
          return null;
        }
      });
      List retJSON = new ArrayList<>(ekvs.size());
      for (EncryptedKeyVersion ekv: ekvs) {
        retJSON.add(KMSUtil.toJSON(ekv));
      }
      kmsAudit.ok(user, KMSOp.REENCRYPT_EEK_BATCH, name,
          "reencrypted " + ekvs.size() + " keys");
      LOG.info("reencryptEncryptedKeys {} keys for key {} took {}",
          jsonPayload.size(), name, sw.stop());
      LOG.trace("Exiting reencryptEncryptedKeys method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(retJSON)
          .build();
    } catch (Exception e) {
      LOG.debug("Exception in reencryptEncryptedKeys.", e);
      throw e;
    }
  }

  @SuppressWarnings("rawtypes")
  @POST
  @Path(KMSRESTConstants.KEY_VERSION_RESOURCE + "/{versionName:.*}/" +
      KMSRESTConstants.EEK_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response handleEncryptedKeyOp(
      @PathParam("versionName") final String versionName,
      @QueryParam(KMSRESTConstants.EEK_OP) String eekOp,
      Map jsonPayload)
      throws Exception {
    try {
      LOG.trace("Entering decryptEncryptedKey method.");
      UserGroupInformation user = HttpUserGroupInformation.get();
      checkNotEmpty(versionName, "versionName");
      checkNotNull(eekOp, "eekOp");
      LOG.debug("Decrypting key for {}, the edek Operation is {}.",
              versionName, eekOp);

      final String keyName = (String) jsonPayload.get(
              KMSRESTConstants.NAME_FIELD);
      String ivStr = (String) jsonPayload.get(KMSRESTConstants.IV_FIELD);
      String encMaterialStr =
              (String) jsonPayload.get(KMSRESTConstants.MATERIAL_FIELD);
      checkNotNull(ivStr, KMSRESTConstants.IV_FIELD);
      final byte[] iv = Base64.decodeBase64(ivStr);
      checkNotNull(encMaterialStr,
          KMSRESTConstants.MATERIAL_FIELD);
      final byte[] encMaterial = Base64.decodeBase64(encMaterialStr);
      Object retJSON;
      if (eekOp.equals(KMSRESTConstants.EEK_DECRYPT)) {
        KMSWebApp.getDecryptEEKCallsMeter().mark();
        assertAccess(KMSACLs.Type.DECRYPT_EEK, user, KMSOp.DECRYPT_EEK,
                keyName);

        KeyProvider.KeyVersion retKeyVersion = user.doAs(
                new PrivilegedExceptionAction<KeyVersion>() {
                @Override
                  public KeyVersion run() throws Exception {
                  return provider.decryptEncryptedKey(
                            new KMSClientProvider.KMSEncryptedKeyVersion(
                                    keyName, versionName, iv,
                                            KeyProviderCryptoExtension.EEK,
                                            encMaterial)
                    );
                }
              }
        );

        retJSON = KMSUtil.toJSON(retKeyVersion);
        kmsAudit.ok(user, KMSOp.DECRYPT_EEK, keyName, "");
      } else if (eekOp.equals(KMSRESTConstants.EEK_REENCRYPT)) {
        KMSWebApp.getReencryptEEKCallsMeter().mark();
        assertAccess(KMSACLs.Type.GENERATE_EEK, user, KMSOp.REENCRYPT_EEK,
            keyName);

        EncryptedKeyVersion retEncryptedKeyVersion =
            user.doAs(new PrivilegedExceptionAction<EncryptedKeyVersion>() {
              @Override
              public EncryptedKeyVersion run() throws Exception {
                return provider.reencryptEncryptedKey(
                    new KMSClientProvider.KMSEncryptedKeyVersion(keyName,
                        versionName, iv, KeyProviderCryptoExtension.EEK,
                        encMaterial));
              }
            });

        retJSON = KMSUtil.toJSON(retEncryptedKeyVersion);
        kmsAudit.ok(user, KMSOp.REENCRYPT_EEK, keyName, "");
      } else {
        StringBuilder error;
        error = new StringBuilder("IllegalArgumentException Wrong ");
        error.append(KMSRESTConstants.EEK_OP);
        error.append(" value, it must be ");
        error.append(KMSRESTConstants.EEK_GENERATE);
        error.append(" or ");
        error.append(KMSRESTConstants.EEK_DECRYPT);
        LOG.error(error.toString());
        throw new IllegalArgumentException(error.toString());
      }
      LOG.trace("Exiting handleEncryptedKeyOp method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(retJSON)
          .build();
    } catch (Exception e) {
      LOG.debug("Exception in handleEncryptedKeyOp.", e);
      throw e;
    }
  }

  @GET
  @Path(KMSRESTConstants.KEY_RESOURCE + "/{name:.*}/" +
      KMSRESTConstants.VERSIONS_SUB_RESOURCE)
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getKeyVersions(@PathParam("name") final String name)
      throws Exception {
    try {
      LOG.trace("Entering getKeyVersions method.");
      UserGroupInformation user = HttpUserGroupInformation.get();
      checkNotEmpty(name, "name");
      KMSWebApp.getKeyCallsMeter().mark();
      assertAccess(KMSACLs.Type.GET, user, KMSOp.GET_KEY_VERSIONS, name);
      LOG.debug("Getting key versions for key {}", name);

      List<KeyVersion> ret = user.doAs(
              new PrivilegedExceptionAction<List<KeyVersion>>() {
              @Override
                public List<KeyVersion> run() throws Exception {
                return provider.getKeyVersions(name);
              }
            }
      );

      Object json = KMSServerJSONUtils.toJSON(ret);
      kmsAudit.ok(user, KMSOp.GET_KEY_VERSIONS, name, "");
      LOG.trace("Exiting getKeyVersions method.");
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(json)
              .build();
    } catch (Exception e) {
      LOG.debug("Exception in getKeyVersions.", e);
      throw e;
    }
  }

}
