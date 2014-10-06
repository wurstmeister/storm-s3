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
package org.apache.storm.s3.output.trident;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.storm.s3.output.S3Configuration.*;
import static org.apache.storm.s3.output.S3Configuration.ROTATION_SIZE;
import static org.apache.storm.s3.output.S3Configuration.ROTATION_UNIT;
import static org.junit.Assert.assertEquals;

public class DefaultS3TransactionalOutputTest {

    private Config config;
    private TransferManager tx;
    private FileOutputFactory fileOutputFactory;
    private String bucketName;

    @Before
    public void setup() {
        config = new Config();
        config.put(PREFIX, "trident");
        config.put(EXTENSION, ".txt");
        config.put(PATH, "trident");
        config.put(OUTPUT_FIELDS, Arrays.asList("sentence"));
        config.put(ROTATION_SIZE, 1.0);
        config.put(ROTATION_UNIT, "KB");
        AWSCredentialsProvider provider = new ProfileCredentialsProvider("aws-testing");
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        AmazonS3 client = new AmazonS3Client(provider.getCredentials(), clientConfiguration);
        tx = new TransferManager(client);
        bucketName = System.currentTimeMillis() + "-test-transactional-output-bucket";
        fileOutputFactory = new DefaultFileOutputFactory<Long>() {

            @Override
            public String buildBucketName(Long key) {
                return bucketName;
            }

            @Override
            public String buildPath(Long key) {
                return key.toString();
            }
        };
    }

    @After
    public void cleanup () {
        AmazonS3 client = tx.getAmazonS3Client();
        if (client.doesBucketExist(bucketName)) {
            ObjectListing objectListing = client.listObjects(bucketName);
            List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
            for (S3ObjectSummary summary : objectSummaries) {
                client.deleteObject(bucketName, summary.getKey());
            }
            client.deleteBucket(bucketName);
        }
    }

    @Test
      public void testWrite() throws Exception {
        S3TransactionalOutput<Long> output = new DefaultS3TransactionalOutput<Long>(1L, config, tx, fileOutputFactory);
        output.prepare(config);
        List<TridentTuple> tuples = Arrays.asList(TridentTupleView.createFreshTuple(new Fields("sentence"), "a"));
        for ( int i = 0 ; i < 513 ; i++) {
            output.write(tuples, (long)i);
        }
        String bucketName = fileOutputFactory.buildBucketName(1L);
        assertCorrectFileSize(bucketName, 512);
    }

    @Test
    public void testWriteOnlyOnTransactionBoundary() throws Exception {
        S3TransactionalOutput<Long> output = new DefaultS3TransactionalOutput<Long>(1L, config, tx, fileOutputFactory);
        output.prepare(config);
        List<TridentTuple> tuples = Arrays.asList(TridentTupleView.createFreshTuple(new Fields("sentence"), "a"));
        for ( int i = 0 ; i < 514 ; i++) {
            output.write(tuples, 1L);
        }
        output.write(tuples, 2L);
        String bucketName = fileOutputFactory.buildBucketName(1L);
        assertCorrectFileSize(bucketName, 514);

    }

    private void assertCorrectFileSize(String bucketName, int size) throws IOException {
        AmazonS3 client = tx.getAmazonS3Client();
        ObjectListing objectListing = client.listObjects(bucketName);
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        assertEquals(1, objectSummaries.size());
        S3ObjectSummary s3ObjectSummary = objectSummaries.get(0);
        File tempFile = File.createTempFile("test", "txt");
        FileUtils.copyInputStreamToFile(client.getObject(bucketName, s3ObjectSummary.getKey()).getObjectContent(), tempFile);
        List<String> lines = FileUtils.readLines(tempFile);
        assertEquals(size, lines.size());
        client.deleteObject(bucketName, s3ObjectSummary.getKey());
        client.deleteBucket(bucketName);
    }
}
