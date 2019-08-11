package org.apache.hadoop.ozone.om.helpers;

import java.util.ArrayList;
import java.util.List;

/**
 * List of in-flight MU upoads.
 */
public class OmMultipartUploadList {

  private List<OmMultipartUpload> uploads = new ArrayList<>();

  public OmMultipartUploadList(
      List<OmMultipartUpload> uploads) {
    this.uploads = uploads;
  }

  public List<OmMultipartUpload> getUploads() {
    return uploads;
  }

  public void setUploads(
      List<OmMultipartUpload> uploads) {
    this.uploads = uploads;
  }
}
