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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.FileNameFormat;
import org.apache.storm.s3.output.S3Configuration;
import org.apache.storm.s3.output.S3MemBufferedOutputStream;
import org.apache.storm.s3.output.TransferManagerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for ensuring that output is only written on transaction boundaries.
 */
public abstract class S3TransactionalOutput<T> {

    private static final Logger LOG = LoggerFactory.getLogger(S3TransactionalOutput.class);

    protected final S3Configuration configuration;
    private T key;
    private FileOutputFactory<T> fileOutputFactory;
    protected S3MemBufferedOutputStream out;
    protected TransferManager transferManager;
    private String bucketName;
    protected long transactionId = 1L;
    protected long latestTransactionInOutput = 1L;
    protected long previouslyClosedTransaction = 1L;
    protected boolean canRotate;


    public S3TransactionalOutput(T key, Map conf, TransferManager transferManager, FileOutputFactory<T> fileOutputFactory) {
        this(conf);
        this.transferManager = transferManager;
        this.fileOutputFactory = fileOutputFactory;
        this.key = key;
    }

    public S3TransactionalOutput(Map conf) {
        configuration = new S3Configuration(conf);
    }

    public void prepare(Map conf) throws IOException {
        String bucket = bucketName();
        if (bucket == null) {
            throw new IllegalStateException("Bucket name must be specified.");
        }
        LOG.info("Preparing S3 Output for bucket {}", bucket);
        transferManager = getTransferManager(conf);
        AmazonS3 amazonS3Client = transferManager.getAmazonS3Client();
        if (!amazonS3Client.doesBucketExist(bucket)) {
            amazonS3Client.createBucket(bucket);
            LOG.info("Creating bucket {}", bucket);
        }
        LOG.info("Prepared S3 Output for bucket {} ", bucket);
        restoreState();
        createOutputFile();
    }

    private TransferManager getTransferManager(Map conf) {
        if (transferManager == null) {
            transferManager = TransferManagerBuilder.buildTransferManager(conf);
        }
        return transferManager;
    }

    public S3TransactionalOutput withBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    private void write(TridentTuple tuple) throws IOException {
        byte[] bytes = configuration.getRecordFormat().format(tuple);
        if (canRotate && shouldRotate()) {
            rotateOutputFile();
            configuration.getRotationPolicy().reset();
        }
        out.write(bytes);
        configuration.getRotationPolicy().mark(bytes.length);
    }

    private boolean shouldRotate() {
        return configuration.getRotationPolicy().mark(0);
    }

    private void rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        closeOutputFile();
        LOG.info("closed {} with transactions {} - {}", new Object[]{bucketName(), previouslyClosedTransaction, latestTransactionInOutput});
        previouslyClosedTransaction = latestTransactionInOutput;
        canRotate = false;
        createOutputFile();
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }

    protected final void write(TridentTuple tuple, long txId) throws IOException {
        if (txId != transactionId) {
            LOG.debug("new transaction id {} --> {} -- {}, safe to rotate file", new Object[]{txId, transactionId, bucketName()});
            canRotate = true;
            latestTransactionInOutput = transactionId;
        }
        this.transactionId = txId;
        write(tuple);
    }

    private void createOutputFile() throws IOException {
        FileNameFormat format = configuration.getFileNameFormat();
        if ( fileOutputFactory != null && key != null) {
            format = new DefaultFileNameFormat().withPath(fileOutputFactory.buildPath(key)).withPrefix(fileOutputFactory.buildPrefix(key)).withExtension(configuration.getExtension());
        }

        this.out = new S3MemBufferedOutputStream(transferManager, bucketName(), format,
                configuration.getContentType(), latestTransactionInOutput + "");
    }

    protected String bucketName(){
        String bucket = bucketName;
        if ( fileOutputFactory != null && key != null) {
            bucket = fileOutputFactory.buildBucketName(key);
        }
        return bucket;
    }

    public Long getTransactionId() {
        return transactionId;
    }

    protected void closeOutputFile() throws IOException {
        this.out.close(latestTransactionInOutput);
    }

    protected T getKey() {
        return key;
    }

    public abstract void write(T keyLong, List<TridentTuple> tuples, Long txid) throws IOException;

    public abstract void write(List<TridentTuple> tuples, Long txid) throws IOException;

    /**
     * this method should be implemented by any implementation that allows
     * state to be restored at startup
     *
     * @throws IOException
     */
    protected void restoreState() throws IOException{

    }

}
