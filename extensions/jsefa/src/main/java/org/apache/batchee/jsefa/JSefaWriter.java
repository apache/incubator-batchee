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
package org.apache.batchee.jsefa;

import net.sf.jsefa.Serializer;
import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.TransactionalWriter;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import java.io.File;
import java.io.Serializable;
import java.util.List;

public abstract class JSefaWriter implements ItemWriter {
    @Inject
    @BatchProperty
    @Documentation("object types to use")
    protected String objectTypes;

    @Inject
    @BatchProperty
    @Documentation("validation mode (AUTO, CALLBACK, NONE)")
    protected String validationMode;

    @Inject
    @BatchProperty
    @Documentation("object accessor provider implementation")
    protected String objectAccessorProvider;

    @Inject
    @BatchProperty
    @Documentation("validation provider implementation")
    protected String validationProvider;

    @Inject
    @BatchProperty
    @Documentation("simple type provider implementation")
    protected String simpleTypeProvider;

    @Inject
    @BatchProperty
    @Documentation("type mapping registry to use")
    protected String typeMappingRegistry;

    @Inject
    @BatchProperty
    @Documentation("file to write")
    protected String file;

    @Inject
    @BatchProperty
    @Documentation("output file encoding")
    protected String encoding;

    protected Serializer serializer;
    protected TransactionalWriter transactionalWriter;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final File f = new File(file);
        if (!f.getParentFile().exists() && !f.getParentFile().mkdirs()) {
            throw new BatchRuntimeException(f.getParentFile().getAbsolutePath());
        }

        serializer = createSerializer();
        transactionalWriter = new TransactionalWriter(f, encoding, checkpoint);
        serializer.open(transactionalWriter);
    }

    protected abstract Serializer createSerializer() throws Exception;

    @Override
    public void close() throws Exception {
        if (serializer != null) {
            serializer.close(true);
        }
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object item : items) {
            serializer.write(item);
        }
        transactionalWriter.flush();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return transactionalWriter.position();
    }
}
