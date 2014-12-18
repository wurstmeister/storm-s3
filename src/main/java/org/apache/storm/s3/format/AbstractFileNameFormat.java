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
package org.apache.storm.s3.format;

import org.apache.storm.s3.output.trident.FileOutputFactory;

/**
 * Created by tastle on 17/12/2014.
 */
public abstract class AbstractFileNameFormat {

    protected String delimiter = "-";
    protected String path = "storm/";
    protected String prefix = "";
    protected String extension = ".txt";
    private FileOutputFactory fileOutputFactory = null;

    /**
     * Overrides the default prefix.
     *
     * @param factory
     * @return
     */
    public AbstractFileNameFormat withFileOutputFactory(FileOutputFactory factory) {
        this.fileOutputFactory = factory;
        return this;
    }

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public AbstractFileNameFormat withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public AbstractFileNameFormat withExtension(String extension) {
        this.extension = extension;
        return this;
    }

    /**
     * Overrides the default delimiter.
     *
     * @param delimiter
     * @return
     */
    public AbstractFileNameFormat withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public AbstractFileNameFormat withPath(String path) {
        if (null == path) {
            path = "";
        }
        this.path = path;
        if (!this.path.endsWith("/")) {
            this.path = this.path + "/";
        }
        return this;
    }

    /**
     * Returns the filename the S3Bolt will create.
     *
     * @param key
     * @param identifier a unique identifier provided for the writer of the file
     * @param rotation   the current file rotation number (incremented on every rotation)
     * @param timeStamp  current time in milliseconds when the rotation occurs
     * @return
     */
    public abstract String getName(Object key, String identifier, long rotation, long timeStamp);

    /**
     * @return the <code>FileOutputFactory</code> or <code>null</code> if none is set.
     */
    public FileOutputFactory getFileOutputFactory() {
        return this.fileOutputFactory;
    }
}
