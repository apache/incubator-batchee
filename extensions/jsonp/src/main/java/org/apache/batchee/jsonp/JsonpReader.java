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
import org.apache.batchee.extras.transaction.CountedReader;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonParser;
import java.io.FileInputStream;
import java.io.Serializable;

@Documentation("Reads a JSON file using JSON-P providing JsonStructure as item.")
public class JsonpReader extends CountedReader {
    @Inject
    @BatchProperty
    @Documentation("Incoming file")
    private String file;

    @Inject
    @BatchProperty
    @Documentation("Should root be skipped (default: true)")
    private String skipRoot;

    @Inject
    @BatchProperty
    @Documentation("JSON-P provider if not using the default")
    private String provider;

    private JsonParser parser;
    private JsonPartialReader reader;
    private JsonParser.Event end = null;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final JsonProvider provider = this.provider == null ? JsonProvider.provider() : JsonProvider.class.cast(loader.loadClass(this.provider));
        parser = provider.createParser(new FileInputStream(file));
        reader = new JsonPartialReader(provider, parser);

        if (skipRoot == null || "true".equalsIgnoreCase(skipRoot)) {
            final JsonParser.Event event = parser.next();
            if (event == JsonParser.Event.START_ARRAY) {
                end = JsonParser.Event.END_ARRAY;
            } else {
                end = JsonParser.Event.END_OBJECT;
            }
        }
        super.open(checkpoint);
    }

    @Override
    protected Object doRead() throws Exception {
        JsonParser.Event event;
        do {
            event = parser.next();
        } while (
            (event != JsonParser.Event.START_OBJECT && event != end) ||
            (end == null && (event == JsonParser.Event.END_ARRAY ||
            event == JsonParser.Event.END_OBJECT)));
        if (!parser.hasNext()) {
            return null;
        }

        return reader.read(event);
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }
}
