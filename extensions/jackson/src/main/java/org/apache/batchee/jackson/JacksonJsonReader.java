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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.CountedReader;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import java.io.File;
import java.io.Serializable;

@Documentation("Reads a JSON file using jackson.")
public class JacksonJsonReader extends CountedReader {
    @Inject
    @BatchProperty
    @Documentation("Incoming file")
    private String file;

    @Inject
    @BatchProperty
    @Documentation("Type to instantiate")
    private String type;

    @Inject
    @BatchProperty
    @Documentation("DeserializationFeature and SerializationFeature comma separated key-value configurations")
    private String configuration;

    @Inject
    @BatchProperty
    @Documentation("Should root be skipped (default: true)")
    private String skipRoot;

    private JsonParser parser;
    private Class<?> clazz;
    private JsonToken end = null;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final ObjectMapper mapper = Jacksons.newMapper(configuration);
        parser = mapper.getFactory().createParser(new File(file));
        if (type != null) {
            clazz = Thread.currentThread().getContextClassLoader().loadClass(type);
        } else {
            clazz = null;
        }

        if (skipRoot == null || "true".equalsIgnoreCase(skipRoot)) {
            final JsonToken token = parser.nextToken();
            if (token == JsonToken.START_ARRAY) {
                end = JsonToken.END_ARRAY;
            } else {
                end = JsonToken.END_OBJECT;
            }
        }
        super.open(checkpoint);
    }

    @Override
    protected Object doRead() throws Exception {
        JsonToken token;
        do {
            token = parser.nextToken();
        } while ((token != JsonToken.START_OBJECT && token != end) || (end == null && (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT)));

        if (clazz == null) {
            parser.readValueAsTree();
        }
        return parser.readValueAs(clazz);
    }

    @Override
    public void close() throws Exception {
        if (parser != null) {
            parser.close();
        }
    }
}
