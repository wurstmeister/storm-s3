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
import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


/**
 * OutputStream that buffers data in memory before writing it to S3
 */
public class S3MemBufferedOutputStream<T> extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3MemBufferedOutputStream.class);

    private final String bucketName;
    private final String contentType;
    private final AbstractFileNameFormat fileNameFormat;
    private final ByteArrayOutputStream outputStream;
    private final Uploader uploader;

    public S3MemBufferedOutputStream(Uploader uploader, String bucketName,
                                     AbstractFileNameFormat fileNameFormat, String contentType) {

        this.outputStream = new ByteArrayOutputStream();
        this.uploader = uploader;
        this.bucketName = bucketName;
        this.fileNameFormat = fileNameFormat;
        this.contentType = contentType;
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void close() throws IOException  {
        close(0);
    }

    public void close(long rotation) throws IOException {
        close(null, "", rotation);
    }

    public void close(T key, String identifier, long rotation) throws IOException {
        String name = fileNameFormat.getName(key, identifier, rotation, System.currentTimeMillis());
        LOG.info("uploading {}/{} to S3",bucketName, name);
        outputStream.close();
        final byte[] buf = outputStream.toByteArray();
        InputStream input = new ByteArrayInputStream(buf);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType(contentType);
        meta.setContentLength(buf.length);
        uploader.upload(key, bucketName, name, input, meta);
        input.close();
    }
}
