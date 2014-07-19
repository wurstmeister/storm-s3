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
package org.apache.storm.s3;

import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.storm.s3.format.FileNameFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.output.MemBufferedS3OutputStream;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

public class S3Output implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(S3Output.class);

    private FileRotationPolicy rotationPolicy;
    private FileNameFormat fileNameFormat;
    private int rotation = 0;
    private long offset = 0;
    private OutputStream out;
    private RecordFormat format;
    private TransferManager transferManager;
    private String bucketName;
    private String contentType = "text/plain";
    private String identifier;

    public S3Output withFileNameFormat(FileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public S3Output withRecordFormat(RecordFormat format) {
        this.format = format;
        return this;
    }

    public S3Output withRotationPolicy(FileRotationPolicy rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public S3Output withBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public S3Output withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public S3Output withIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    public RecordFormat getRecordFormat() {
        return format;
    }

    public void prepare(Map conf) throws IOException {
        LOG.info("Preparing S3 Output ...");
        if (this.bucketName == null) {
            throw new IllegalStateException("Bucket name must be specified.");
        }
        transferManager = TransferManagerBuilder.buildTransferManager(conf);
        transferManager.getAmazonS3Client().createBucket(bucketName);
        createOutputFile();
    }

    public void write(byte[] bytes) throws IOException {
        out.write(bytes);
        this.offset += bytes.length;
        if (this.rotationPolicy.mark(this.offset)) {
            rotateOutputFile();
            this.offset = 0;
            this.rotationPolicy.reset();
        }
    }


    private void rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        closeOutputFile();
        this.rotation++;
        createOutputFile();
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }

    private void createOutputFile() throws IOException {
        this.out = new MemBufferedS3OutputStream(transferManager, bucketName, fileNameFormat, contentType, rotation, identifier);
    }

    private void closeOutputFile() throws IOException {
        this.out.close();
    }
}
