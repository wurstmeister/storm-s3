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

import java.io.Serializable;

/**
 * Allows control of the S3 Bucket files are written to. The file name is controlled by the
 * implementation of <Code>org.apache.storm.s3.format.FileNameFormat</Code>
 *
 * The S3 bucket name will generated using:
 * <code>buildBucketName()</code>
 *
 * @param <T>
 */
public interface FileOutputFactory<T> extends Serializable {
    String buildBucketName(T key);
    String buildPath(T key);
    String buildPrefix(T key);
}
