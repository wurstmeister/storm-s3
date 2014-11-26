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

import backtype.storm.tuple.Fields;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.DelimitedRecordFormat;
import org.apache.storm.s3.format.FileNameFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.util.List;
import java.util.Map;

public class S3Configuration {

    private static final Logger LOG = LoggerFactory.getLogger(S3Configuration.class);

    public static final String PREFIX = "PREFIX";
    public static final String EXTENSION = "EXTENSION";
    public static final String PATH = "PATH";
    public static final String OUTPUT_FIELDS = "OUTPUT_FIELDS";
    /** The size a file can reach before rotation, must be specified as a <code>Double</code> eg 10.0 */
    public static final String ROTATION_SIZE = "ROTATION_SIZE";
    public static final String ROTATION_UNIT = "ROTATION_UNIT";
    public static final String BUCKET_NAME = "BUCKET_NAME";
    public static final String CONTENT_TYPE = "CONTENT_TYPE";
    public static final String FILE_NAME_FORMAT_CLASS = "FILE_NAME_FORMAT_CLASS";

    /** The default file size rotation policy if no configuration is provided */
    public static final FileSizeRotationPolicy DEFAULT_ROTATION_POLICY =
        new FileSizeRotationPolicy(10.0F, FileSizeRotationPolicy.Units.MB);

    private FileRotationPolicy rotationPolicy;
    private FileNameFormat fileNameFormat;
    private RecordFormat format;
    private String bucketName;
    private String extension;
    private String contentType;


    public S3Configuration(Map conf) {

        extension = (String) conf.get(EXTENSION);
        contentType = (String) conf.get(CONTENT_TYPE);
        fileNameFormat = getFileNameFormat(conf);

        Fields fields = null;
        if (conf.containsKey(OUTPUT_FIELDS)) {
            List fieldsList = (List) conf.get(OUTPUT_FIELDS);
            fields = new Fields(fieldsList);
        }

        format = new DelimitedRecordFormat().withFields(fields);
        rotationPolicy = getFileRotationPolicy(conf);

        if (conf.containsKey(BUCKET_NAME)) {
            bucketName = (String) conf.get(BUCKET_NAME);
        }
    }

    public FileNameFormat getFileNameFormat() {
        return fileNameFormat;
    }

    public FileRotationPolicy getRotationPolicy() {
        return rotationPolicy;
    }

    public RecordFormat getRecordFormat() {
        return format;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getExtension() {
        return extension;
    }

    public String getContentType() {
        return contentType;
    }

    /**
     * @param s3Conf configuration used to build the <Code>S3Configuration</Code>
     * @return the <Code>FileNameFormat</Code> instance if <Code>FILE_NAME_FORMAT_CLASS</Code> was provided.
     *         Otherwise return the default.
     */
    private static FileNameFormat getFileNameFormat(final Map s3Conf) {

        String prefix = (String) s3Conf.get(PREFIX);
        String path = (String) s3Conf.get(PATH);
        String extension = (String) s3Conf.get(EXTENSION);

        FileNameFormat format = new DefaultFileNameFormat().withExtension(extension).withPath(path).withPrefix(prefix);

        if (s3Conf.containsKey(FILE_NAME_FORMAT_CLASS)) {
            String className = (String) s3Conf.get(FILE_NAME_FORMAT_CLASS);
            try {
                Class clazz = Class.forName(className);
                format = (FileNameFormat) clazz.newInstance();
            } catch (Exception e) {
                LOG.error("Failed to construct file name formatter, falling back to default", e);
            }
        }

        return format;
    }

    /**
     * @param s3Conf configuration used to build the <Code>S3Configuration</Code>
     * @return a <Code>FileSizeRotationPolicy</Code> constructed from the Configuration.
     *         Otherwise returns the default.
     */
    private static FileSizeRotationPolicy getFileRotationPolicy(final Map s3Conf) {

        FileSizeRotationPolicy policy = DEFAULT_ROTATION_POLICY;

        if (s3Conf.containsKey(ROTATION_SIZE) && s3Conf.containsKey(ROTATION_UNIT)) {
            try {
                Object size = s3Conf.get(ROTATION_SIZE);
                Float rotationSize = ((Double) size).floatValue();

                FileSizeRotationPolicy.Units rotationUnits = FileSizeRotationPolicy.Units.valueOf((String) s3Conf.get(ROTATION_UNIT));
                policy = new FileSizeRotationPolicy(rotationSize, rotationUnits);
            }
            catch (Exception e) {
                LOG.error("Failed to construct rotation policy, falling back to default", e);
            }
        }

        return policy;
    }

}
