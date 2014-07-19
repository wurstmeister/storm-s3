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
package org.apache.storm.s3.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.net.MediaType;
import org.apache.storm.s3.TransferManagerBuilder;
import org.apache.storm.s3.bolt.format.FileNameFormat;
import org.apache.storm.s3.bolt.format.RecordFormat;
import org.apache.storm.s3.bolt.rotation.FileRotationPolicy;
import org.apache.storm.s3.output.MemBufferedS3OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class S3Bolt extends AbstractS3Bolt {

    private static final Logger LOG = LoggerFactory.getLogger(S3Bolt.class);

    private OutputStream out;
    private RecordFormat format;
    private long offset = 0;
    private TransferManager transferManager;
    private String bucketName;
    private String contentType = "text/plain";

    public S3Bolt withFileNameFormat(FileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public S3Bolt withRecordFormat(RecordFormat format) {
        this.format = format;
        return this;
    }

    public S3Bolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public S3Bolt withBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public S3Bolt withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing S3 Bolt...");
        if (this.bucketName == null) {
            throw new IllegalStateException("Bucket name must be specified.");
        }
        transferManager = TransferManagerBuilder.buildTransferManager(conf);
        transferManager.getAmazonS3Client().createBucket(bucketName);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            byte[] bytes = this.format.format(tuple);
            out.write(bytes);
            this.offset += bytes.length;

            this.collector.ack(tuple);
            if (this.rotationPolicy.mark(tuple, this.offset)) {
                rotateOutputFile();
                this.offset = 0;
                this.rotationPolicy.reset();
            }
        } catch (IOException e) {
            LOG.warn("write/sync failed.", e);
            this.collector.fail(tuple);
        }
    }

    @Override
    void closeOutputFile() throws IOException {
        this.out.close();
    }

    @Override
    void createOutputFile() throws IOException {
        this.out = new MemBufferedS3OutputStream(transferManager, bucketName, fileNameFormat, contentType, rotation);
    }
}
