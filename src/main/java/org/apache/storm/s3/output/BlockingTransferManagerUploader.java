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


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class BlockingTransferManagerUploader extends Uploader {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingTransferManagerUploader.class);

    private TransferManager tx;

    public BlockingTransferManagerUploader(AmazonS3 client){
        super(client);
        this.tx = new TransferManager(client);
    }

    @Override
    public void upload(String bucketName, String name, InputStream input, ObjectMetadata meta) throws IOException {
        final Upload myUpload = tx.upload(bucketName, name, input, meta);
        try {
            UploadResult uploadResult = myUpload.waitForUploadResult();
            LOG.info("Upload completed, bucket={}, key={}", uploadResult.getBucketName(), uploadResult.getKey());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
