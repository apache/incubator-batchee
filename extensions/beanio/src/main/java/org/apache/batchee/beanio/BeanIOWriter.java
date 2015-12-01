/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.beanio;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.TransactionalWriter;
import org.beanio.BeanWriter;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import java.io.File;
import java.io.Serializable;
import java.util.List;

@Documentation("Reads a file using BeanIO.")
public class BeanIOWriter implements ItemWriter {
    @Inject
    @BatchProperty(name = "file")
    @Documentation("The file to write")
    protected String filePath;

    @Inject
    @BatchProperty
    @Documentation("The BeanIO stream name")
    protected String streamName;

    @Inject
    @BatchProperty
    @Documentation("The configuration path in the classpath")
    protected String configuration;

    @Inject
    @BatchProperty
    @Documentation("The output encoding")
    protected String encoding;

    private BeanWriter writer;
    private TransactionalWriter transactionalWriter;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        if (encoding == null) {
            encoding = "UTF-8";
        }

        final File file = new File(filePath);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new BatchRuntimeException(file.getParentFile().getAbsolutePath());
        }

        transactionalWriter = new TransactionalWriter(file, encoding, checkpoint);
        writer = BeanIOs.open(filePath, streamName, configuration).createWriter(streamName, transactionalWriter);
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object item : items) {
            writer.write(item);
        }
        transactionalWriter.flush();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return transactionalWriter.position();
    }
}
