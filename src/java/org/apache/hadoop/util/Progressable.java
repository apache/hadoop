package org.apache.hadoop.util;

import java.io.IOException;



/**
 * An interface for callbacks when an method makes some progress.
 * @author Owen O'Malley
 */
public interface Progressable {
    /** callback for reporting progress. Used by DFSclient to report
     * progress while writing a block of DFS file.
     */
    public void progress() throws IOException;
}