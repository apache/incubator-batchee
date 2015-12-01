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
package org.apache.batchee.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.TransactionalWriter;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import java.io.File;
import java.io.Serializable;
import java.util.List;

@Documentation("Write a JSON file using jackson")
public class JacksonJSonWriter implements ItemWriter {
    @Inject
    @BatchProperty
    @Documentation("output file")
    private String file;

    @Inject
    @BatchProperty
    @Documentation("output encoding")
    private String encoding;

    @Inject
    @BatchProperty
    @Documentation("DeserializationFeature and SerializationFeature comma separated key-value configurations")
    private String configuration;

    @Inject
    @BatchProperty
    @Documentation("is the array wrapped in an object or not")
    private String skipRoot;

    @Inject
    @BatchProperty
    @Documentation("how to generate field names for each item, default uses item1, item2, ...")
    private String fieldNameGeneratorClass;

    private JsonGenerator generator;
    private TransactionalWriter writer;
    private FieldNameGenerator fieldNameGenerator = null;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final File outputFile = new File(file);
        if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()) {
            throw new BatchRuntimeException("Can't create " + outputFile.getAbsolutePath());
        }

        final ObjectMapper mapper = Jacksons.newMapper(configuration);

        writer = new TransactionalWriter(outputFile, encoding, checkpoint);
        generator = mapper.getFactory().createGenerator(writer);
        if (fieldNameGeneratorClass != null) {
            if ("default".equals(fieldNameGeneratorClass)) {
                fieldNameGenerator = new DefaultFieldNameGenerator();
            } else {
                fieldNameGenerator = FieldNameGenerator.class.cast(Thread.currentThread().getContextClassLoader().loadClass(fieldNameGeneratorClass).newInstance());
            }
        }

        if (useGlobalWrapper()) {
            if (fieldNameGenerator != null) {
                generator.writeStartObject();
            } else {
                generator.writeStartArray();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (generator != null) {
            if (useGlobalWrapper()) {
                if (fieldNameGenerator != null) {
                    generator.writeEndObject();
                } else {
                    generator.writeEndArray();
                }
            }
            generator.close();
        }
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        for (final Object o : items) {
            if (fieldNameGenerator != null) {
                generator.writeFieldName(fieldNameGenerator.nextName());
            }
            generator.writeObject(o);
        }
        writer.flush();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return writer.position();
    }

    private boolean useGlobalWrapper() {
        return skipRoot == null || !"true".equalsIgnoreCase(skipRoot);
    }
}
