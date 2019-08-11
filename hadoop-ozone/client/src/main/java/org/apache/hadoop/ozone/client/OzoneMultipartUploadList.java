package org.apache.hadoop.ozone.client;

import java.util.ArrayList;
import java.util.List;

/**
 * List of in-flight MU upoads.
 */
public class OzoneMultipartUploadList {

  private List<OzoneMultipartUpload> uploads = new ArrayList<>();

  public OzoneMultipartUploadList(
      List<OzoneMultipartUpload> uploads) {
    this.uploads = uploads;
  }

  public List<OzoneMultipartUpload> getUploads() {
    return uploads;
  }

  public void setUploads(
      List<OzoneMultipartUpload> uploads) {
    this.uploads = uploads;
  }
}
