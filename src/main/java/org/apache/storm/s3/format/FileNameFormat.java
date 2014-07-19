package org.apache.storm.s3.format;

import java.io.Serializable;

public interface FileNameFormat extends Serializable {

    /**
     * Returns the filename the S3Bolt will create.
     *
     * @param identifier an identifier for the writer of the file
     * @param rotation   the current file rotation number (incremented on every rotation)
     * @param timeStamp  current time in milliseconds when the rotation occurs
     * @return
     */
    String getName(String identifier, long rotation, long timeStamp);
}
