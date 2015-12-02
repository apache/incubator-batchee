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
package org.apache.batchee.jsonp;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.TransactionalWriter;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import javax.json.JsonStructure;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonGenerator;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Documentation("Write a JSON file using JSON-P and taking JsonStructure as items.")
public class JsonpWriter implements ItemWriter {
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
    @Documentation("comma separated key value pairs for the generator factory (converted to a Map<?, ?>)")
    private String configuration;

    @Inject
    @BatchProperty
    @Documentation("is the array wrapped in an object or not")
    private String skipRoot;

    @Inject
    @BatchProperty
    @Documentation("how to generate field names for each item, default uses item1, item2, ...")
    private String fieldNameGeneratorClass;

    @Inject
    @BatchProperty
    @Documentation("JSON-P provider if not using the default")
    private String provider;

    private JsonGenerator generator;
    private TransactionalWriter writer;
    private FieldNameGenerator fieldNameGenerator = null;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final JsonProvider provider = this.provider == null ? JsonProvider.provider() : JsonProvider.class.cast(loader.loadClass(this.provider));

        final File outputFile = new File(file);
        if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()) {
            throw new BatchRuntimeException("Can't create " + outputFile.getAbsolutePath());
        }

        writer = new TransactionalWriter(outputFile, encoding, checkpoint);
        generator = provider.createGeneratorFactory(buildConfig()).createGenerator(writer);
        if (fieldNameGeneratorClass != null) {
            if ("default".equals(fieldNameGeneratorClass)) {
                fieldNameGenerator = new FieldNameGenerator() {
                    private int count = 0;

                    @Override
                    public String nextName() {
                        return "item" + ++count;
                    }
                };
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

    private Map<String, ?> buildConfig() {
        final Map<String, Object> map = new HashMap<String, Object>();
        if (configuration != null) {
            for (final String entry : configuration.trim().split(" *, *")) {
                final String[] parts = entry.split(" *= *");
                if (parts.length != 2) {
                    throw new IllegalArgumentException(entry + " not matching a=b pattern");
                }
                map.put(parts[0], parts[1]);
            }
        }
        return map;
    }

    @Override
    public void close() throws Exception {
        if (generator != null) {
            if (useGlobalWrapper()) {
                generator.writeEnd();
            }
            generator.close();
        }
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        final List<JsonStructure> structures = List.class.cast(items);
        for (final JsonStructure structure : structures) {
            if (fieldNameGenerator != null) {
                generator.write(fieldNameGenerator.nextName(), structure);
            } else {
                generator.write(structure);
            }
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

    public interface FieldNameGenerator {
        String nextName();
    }
}
