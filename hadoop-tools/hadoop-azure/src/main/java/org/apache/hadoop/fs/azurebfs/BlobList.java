package org.apache.hadoop.fs.azurebfs;

import java.util.ArrayList;
import java.util.List;

public class BlobList {

  private List<BlobProperty> blobProperties = new ArrayList<>();

  private String nextMarker;

  void addBlobProperty(final BlobProperty blobProperty) {
    blobProperties.add(blobProperty);
  }

  void setNextMarker(String nextMarker) {
    this.nextMarker = nextMarker;
  }

  public List<BlobProperty> getBlobPropertyList() {
    return blobProperties;
  }

  public String getNextMarker() {
    return nextMarker;
  }

  public BlobList() {

  }
}
