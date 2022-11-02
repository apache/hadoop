package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;

public class MockWithPrefixSASTokenProvider extends MockSASTokenProvider {

    /**
     * Function to return an already generated SAS Token with a '?' prefix
     * @param accountName the name of the storage account.
     * @param fileSystem the name of the fileSystem.
     * @param path the file or directory path.
     * @param operation the operation to be performed on the path.
     * @return
     * @throws IOException
     */
    @Override
    public String getSASToken(String accountName, String fileSystem, String path,
                              String operation) throws IOException {
        String token = super.getSASToken(accountName, fileSystem, path, operation);
        return "?"+token;
    }
}