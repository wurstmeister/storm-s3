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


import com.amazonaws.services.s3.transfer.TransferManager;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SimpleS3TransactionalOutput extends S3TransactionalOutput {


    public SimpleS3TransactionalOutput(Map conf, TransferManager transferManager) {
        super(conf, transferManager);
    }

    public SimpleS3TransactionalOutput(Map conf) {
        super(conf);
    }

    @Override
    public void write(Long key, List<TridentTuple> tuples, Long txid) throws IOException {
        for (TridentTuple tuple : tuples) {
            write(tuple, txid);
        }
    }

    @Override
    public void write(List<TridentTuple> tuples, Long txid) throws IOException {
        for (TridentTuple tuple : tuples) {
            write(tuple, txid);
        }
    }

}
