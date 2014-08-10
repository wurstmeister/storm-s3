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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.output.S3MemBufferedOutputStream;
import org.junit.Test;

import java.io.*;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Requires ~/.aws/credentials file
 * <p/>
 * <p/>
 * [aws-testing]
 * aws_access_key_id=<ACCESS_KEY>
 * aws_secret_access_key=<SECRET_KEY>
 */
public class S3MemBufferedOutputStreamTest {

    @Test
    public void testStream() throws IOException {
        AWSCredentialsProvider provider = new ProfileCredentialsProvider("aws-testing");
        ClientConfiguration config = new ClientConfiguration();
        AmazonS3 client = new AmazonS3Client(provider.getCredentials(), config);

        String bucketName = "test-bucket-" + System.currentTimeMillis();
        client.createBucket(bucketName);
        TransferManager tx = new TransferManager(client);
        OutputStream outputStream = new S3MemBufferedOutputStream(tx, bucketName,
                new DefaultFileNameFormat().withPrefix("test"), "text/plain", 1, "test-id");
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        PrintWriter printer = new PrintWriter(writer);
        printer.println("line1");
        printer.println("line2");
        printer.close();

        ObjectListing objectListing = client.listObjects(bucketName);

        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        assertEquals(1, objectSummaries.size());
        S3ObjectSummary s3ObjectSummary = objectSummaries.get(0);
        InputStreamReader reader = new InputStreamReader(client.getObject(bucketName, s3ObjectSummary.getKey()).getObjectContent());
        BufferedReader r = new BufferedReader(reader);
        assertEquals("line1", r.readLine());
        assertEquals("line2", r.readLine());
        client.deleteObject(bucketName, s3ObjectSummary.getKey());
        client.deleteBucket(bucketName);
    }
}
