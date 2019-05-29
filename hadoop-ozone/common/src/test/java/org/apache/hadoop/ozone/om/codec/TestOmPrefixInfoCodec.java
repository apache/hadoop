package org.apache.hadoop.ozone.om.codec;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class test OmPrefixInfoCodec.
 */
public class TestOmPrefixInfoCodec {

  @Rule
  public ExpectedException thrown = ExpectedException.none();


  private OmPrefixInfoCodec codec;

  @Before
  public void setUp() {
    codec = new OmPrefixInfoCodec();
  }

  @Test
  public void testCodecWithIncorrectValues() throws Exception {
    try {
      codec.fromPersistedFormat("random".getBytes(StandardCharsets.UTF_8));
      fail("testCodecWithIncorrectValues failed");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Can't encode the the raw " +
          "data from the byte array", ex);
    }
  }

  @Test
  public void testCodecWithNullDataFromTable() throws Exception {
    thrown.expect(NullPointerException.class);
    codec.fromPersistedFormat(null);
  }


  @Test
  public void testCodecWithNullDataFromUser() throws Exception {
    thrown.expect(NullPointerException.class);
    codec.toPersistedFormat(null);
  }

  @Test
  public void testToAndFromPersistedFormat() throws IOException {

    List<OzoneAcl> acls = new LinkedList<>();
    OzoneAcl ozoneAcl = new OzoneAcl(ACLIdentityType.USER,
        "hive", ACLType.ALL);
    acls.add(ozoneAcl);
    OmPrefixInfo opiSave = OmPrefixInfo.newBuilder()
        .setName("/user/hive/warehouse")
        .setAcls(acls)
        .addMetadata("id", "100")
        .build();

    OmPrefixInfo opiLoad = codec.fromPersistedFormat(
        codec.toPersistedFormat(opiSave));

    assertTrue("Load saved prefix info should match",
        opiLoad.equals(opiSave));
  }
}