/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3.output;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.apache.storm.s3.format.FileNameFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * OutputStream that buffers data in memory before writing it to S3
 */
public class S3MemBufferedOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3MemBufferedOutputStream.class);

    private final String bucketName;
    private final String contentType;
    private final FileNameFormat fileNameFormat;
    private final ByteArrayOutputStream outputStream;
    private final TransferManager tx;
    private long rotation;
    private String identifier;

    public S3MemBufferedOutputStream(TransferManager tx, String bucketName,
                                     FileNameFormat fileNameFormat, String contentType, String identifier) {
        this(tx, bucketName, fileNameFormat, contentType, 0L, identifier);
    }

    public S3MemBufferedOutputStream(TransferManager tx, String bucketName,
                                     FileNameFormat fileNameFormat, String contentType, long rotation, String identifier) {
        this.outputStream = new ByteArrayOutputStream();
        this.tx = tx;
        this.bucketName = bucketName;
        this.fileNameFormat = fileNameFormat;
        this.contentType = contentType;
        this.rotation = rotation;
        this.identifier = identifier;
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void close() throws IOException  {
        close(rotation);
    }

    public void close(long rotation) throws IOException {
        outputStream.close();
        final byte[] buf = outputStream.toByteArray();
        InputStream input = new ByteArrayInputStream(buf);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType(contentType);
        meta.setContentLength(buf.length);
        final Upload myUpload = tx.upload(bucketName, fileNameFormat.getName(identifier, rotation, System.currentTimeMillis()), input, meta);
        try {
            UploadResult uploadResult = myUpload.waitForUploadResult();
            LOG.info("Upload completed, bucket={}, key={}", uploadResult.getBucketName(), uploadResult.getKey());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        input.close();
    }
}
