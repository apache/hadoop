package org.apache.hadoop.fs.s3a.impl;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * TODO list:
 * - Add abstract class + tests for LocalFS
 * - Add tests for this class
 * - Add documentation
 *  - This class
 *  - `filesystem.md`
 * - Remove remaining TODO s
 * - Clean old `innerCopyFromLocalFile` code up
 */
public class CopyFromLocalOperation extends ExecutingStoreOperation<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(
            CopyFromLocalOperation.class);

    private final CopyFromLocalOperationCallbacks callbacks;
    private final boolean deleteSource;
    private final boolean overwrite;
    private final Path source;
    private final Path destination;

    public CopyFromLocalOperation(
            final StoreContext storeContext,
            Path source,
            Path destination,
            boolean deleteSource,
            boolean overwrite,
            CopyFromLocalOperationCallbacks callbacks) {
        super(storeContext);
        this.callbacks = callbacks;
        this.deleteSource = deleteSource;
        this.overwrite = overwrite;
        this.source = source;
        this.destination = destination;
    }

    @Override
    @Retries.RetryTranslated
    public Void execute()
            throws IOException, PathExistsException {
        LOG.debug("Copying local file from {} to {}", source, destination);
        File sourceFile = callbacks.pathToFile(source);

        checkSource(sourceFile);
        prepareDestination(destination, sourceFile, overwrite);
        uploadSourceFromFS();

        if (deleteSource) {
            callbacks.delete(source, true);
        }

        return null;
    }

    private void uploadSourceFromFS()
            throws IOException, PathExistsException {
        RemoteIterator<LocatedFileStatus> localFiles = callbacks
                .listStatusIterator(source, true);

        // After all files are traversed, this set will contain only emptyDirs
        Set<Path> emptyDirs = new HashSet<>();
        List<UploadEntry> entries = new ArrayList<>();
        while (localFiles.hasNext()) {
            LocatedFileStatus sourceFile = localFiles.next();
            Path sourceFilePath = sourceFile.getPath();

            // Directory containing this file / directory isn't empty
            emptyDirs.remove(sourceFilePath.getParent());

            if (sourceFile.isDirectory()) {
                emptyDirs.add(sourceFilePath);
                continue;
            }

            Path destPath = getFinalPath(sourceFilePath);
            // UploadEntries: have a destination path, a file size
            entries.add(new UploadEntry(
                    sourceFilePath,
                    destPath,
                    sourceFile.getLen()));
        }

        if (localFiles instanceof Closeable) {
            ((Closeable) localFiles).close();
        }

        // Sort all upload entries based on size
        entries.sort(new ReverseComparator(new UploadEntry.SizeComparator()));

        int LARGEST_N_FILES = 5;
        final int sortedUploadsCount = Math.min(LARGEST_N_FILES, entries.size());
        List<UploadEntry> uploaded = new ArrayList<>();

        // Take only top most X entries and upload
        for (int uploadNo = 0; uploadNo < sortedUploadsCount; uploadNo++) {
            UploadEntry uploadEntry = entries.get(uploadNo);
            File file = callbacks.pathToFile(uploadEntry.source);
            callbacks.copyFileFromTo(
                    file,
                    uploadEntry.source,
                    uploadEntry.destination);

            uploaded.add(uploadEntry);
        }

        // Shuffle all remaining entries and upload them
        entries.removeAll(uploaded);
        Collections.shuffle(entries);
        for (UploadEntry uploadEntry : entries) {
            File file = callbacks.pathToFile(uploadEntry.source);
            callbacks.copyFileFromTo(
                    file,
                    uploadEntry.source,
                    uploadEntry.destination);
        }

        for (Path emptyDir : emptyDirs) {
            callbacks.createEmptyDir(getFinalPath(emptyDir));
        }
    }

    private void checkSource(File src)
            throws FileNotFoundException {
        if (!src.exists()) {
            throw new FileNotFoundException("No file: " + src.getPath());
        }
    }

    private void prepareDestination(
            Path dst,
            File src,
            boolean overwrite) throws PathExistsException, IOException {
        try {
            FileStatus dstStatus = callbacks.getFileStatus(dst);

            if (src.isFile() && dstStatus.isDirectory()) {
                throw new PathExistsException(
                        "Source '" + src.getPath() +"' is file and " +
                                "destination '" + dst + "' is directory");
            }

            if (!overwrite) {
                throw new PathExistsException(dst + " already exists");
            }
        } catch (FileNotFoundException e) {
            // no destination, all is well
        }
    }

    private Path getFinalPath(Path src) throws IOException {
        URI currentSrcUri = src.toUri();
        URI relativeSrcUri = source.toUri().relativize(currentSrcUri);
        if (currentSrcUri == relativeSrcUri) {
            throw new IOException("Cannot get relative path");
        }

        if (!relativeSrcUri.getPath().isEmpty()) {
            return new Path(destination, relativeSrcUri.getPath());
        } else {
            return new Path(destination, src.getName());
        }
    }

    private static final class UploadEntry {
        private final Path source;
        private final Path destination;
        private final long size;

        private UploadEntry(Path source, Path destination, long size) {
            this.source = source;
            this.destination = destination;
            this.size = size;
        }

        static class SizeComparator implements Comparator<UploadEntry> {
            @Override
            public int compare(UploadEntry entry1, UploadEntry entry2) {
                return Long.compare(entry1.size, entry2.size);
            }
        }
    }

    public interface CopyFromLocalOperationCallbacks {
        RemoteIterator<LocatedFileStatus> listStatusIterator(
                Path f,
                boolean recursive) throws IOException;

        FileStatus getFileStatus( final Path f) throws IOException;

        File pathToFile(Path path);

        boolean delete(Path path, boolean recursive) throws IOException;

        void copyFileFromTo(
                File file,
                Path source,
                Path destination) throws IOException;

        boolean createEmptyDir(Path path) throws IOException;
    }
}
