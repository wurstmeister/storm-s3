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


import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class S3Output implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(S3Output.class);
    private int rotation = 0;
    private S3MemBufferedOutputStream out;
    private Uploader uploader;
    private String contentType = "text/plain";
    private String identifier;

    private S3Configuration configuration;


    public S3Output(Map conf) {
        configuration = new S3Configuration(conf);
    }

    public S3Output withIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    public void prepare(Map conf) throws IOException {
        String bucketName = configuration.getBucketName();
        if (bucketName == null) {
            throw new IllegalStateException("Bucket name must be specified.");
        }
        LOG.info("Preparing S3 Output for bucket {}", bucketName);
        uploader = UploaderFactory.buildUploader(conf);
        uploader.ensureBucketExists(bucketName);
        LOG.info("Prepared S3 Output for bucket {} ", bucketName);
        createOutputFile();
    }

    public void write(Tuple tuple) throws IOException {
        byte[] bytes = configuration.getRecordFormat().format(tuple);
        out.write(bytes);
        if (configuration.getRotationPolicy().mark(bytes.length)) {
            rotateOutputFile();
            configuration.getRotationPolicy().reset();
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
        this.out = new S3MemBufferedOutputStream(uploader, configuration.getBucketName(),
                configuration.getFileNameFormat(), contentType);
    }

    private void closeOutputFile() throws IOException {
        this.out.close(null, identifier, rotation);
    }
}
