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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public abstract class Uploader<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Uploader.class);

    protected AmazonS3 client;

    public Uploader(AmazonS3 client) {
        this.client = client;
    }

    public abstract void upload(String bucketName, String name, InputStream input, ObjectMetadata meta) throws IOException;

    /**
     * By default the key is ignored, but some implementations might need that information
     */
    public void upload(T key, String bucketName, String name, InputStream input, ObjectMetadata meta) throws IOException{
        this.upload(bucketName, name, input, meta);
    }


    public void ensureBucketExists(String bucket) {
        if (!client.doesBucketExist(bucket)) {
            client.createBucket(bucket);
            LOG.info("Creating bucket {}", bucket);
        }
    }


}
