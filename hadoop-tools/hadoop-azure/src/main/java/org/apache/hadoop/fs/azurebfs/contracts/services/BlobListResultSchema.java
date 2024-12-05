package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.ArrayList;
import java.util.List;

/**
 * The ListResultSchema model for Blob Endpoint Listing.
 */
public class BlobListResultSchema implements ListResultSchema {

  // List of paths returned by Blob Endpoint Listing.
  private List<BlobListResultEntrySchema> paths;

  // Continuation token for the next page of results.
  private String nextMarker;

  public BlobListResultSchema() {
    this.paths = new ArrayList<>();
    nextMarker = null;
  }

  /**
   * Return the list of paths returned by Blob Endpoint Listing.
   * @return
   */
  @Override
  public List<BlobListResultEntrySchema> paths() {
    return paths;
  }

  /**
   * Set the paths value to list of paths returned by Blob Endpoint Listing.
   * @param paths the paths value to set
   * @return the ListSchema object itself.
   */
  @Override
  public ListResultSchema withPaths(final List<? extends ListResultEntrySchema> paths) {
    this.paths = (List<BlobListResultEntrySchema>) paths;
    return this;
  }

  public void addBlobListEntry(final BlobListResultEntrySchema blobListEntry) {
    this.paths.add(blobListEntry);
  }

  public String getNextMarker() {
    return nextMarker;
  }

  public void setNextMarker(String nextMarker) {
    this.nextMarker = nextMarker;
  }
}
