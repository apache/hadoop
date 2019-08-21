package org.apache.hadoop.ozone.web.ozShell;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;

import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the json object printer.
 */
public class TestObjectPrinter {

  @Test
  public void printObjectAsJson() throws IOException {

    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneVolume volume =
        new OzoneVolume(conf, Mockito.mock(ClientProtocol.class), "name",
            "admin", "owner", 1L, 0L,
            new ArrayList<>());

    String result = ObjectPrinter.getObjectAsJson(volume);
    Assert.assertTrue("Result is not a proper json",
        result.contains("\"owner\""));
  }
}