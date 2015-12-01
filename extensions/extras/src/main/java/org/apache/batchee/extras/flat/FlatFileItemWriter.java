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
package org.apache.batchee.extras.flat;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.TransactionalWriter;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import java.io.File;
import java.io.Serializable;
import java.util.List;

@Documentation("Writes a flat file.")
public class FlatFileItemWriter implements ItemWriter {
    @Inject
    @BatchProperty
    @Documentation("output file path")
    private String output;

    @Inject
    @BatchProperty
    @Documentation("output file encoding")
    private String encoding;

    @Inject
    @BatchProperty(name = "line.separator")
    @Documentation("line separator to use, default is OS dependent")
    private String lineSeparator;

    private TransactionalWriter writer = null;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        if (output == null) {
            throw new BatchRuntimeException("Can't find any output");
        }
        final File file = new File(output);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new BatchRuntimeException("Can't create parent for " + output);
        }
        if (lineSeparator == null) {
            lineSeparator = System.getProperty("line.separator", "\n");
        } else if ("\\n".equals(lineSeparator)) {
            lineSeparator = "\n";
        } else if ("\\r\\n".equals(lineSeparator)) {
            lineSeparator = "\r\n";
        }

        writer = new TransactionalWriter(file, encoding, checkpoint);
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object item : items) {
            final String string = preWrite(item);
            if (string != null) {
                final String toWrite = string + lineSeparator;
                writer.write(toWrite);
            }
        }
        writer.flush();
    }

    protected String preWrite(final Object object) {
        if (object == null) {
            return null;
        }
        return object.toString();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return writer.position();
    }
}
