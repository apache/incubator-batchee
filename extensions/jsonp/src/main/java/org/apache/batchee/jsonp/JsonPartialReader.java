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

import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonStructure;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParsingException;

public class JsonPartialReader {
    private final JsonParser parser;
    private final JsonProvider provider;
    private boolean closed = false;

    public JsonPartialReader(final JsonProvider provider, final JsonParser parser) {
        this.parser = parser;
        this.provider = provider == null ? JsonProvider.provider() : provider;
    }

    public JsonStructure read(final JsonParser.Event event) {
        switch (event) {
            case START_OBJECT:
                final JsonObjectBuilder objectBuilder = provider.createObjectBuilder();
                parseObject(objectBuilder);
                return objectBuilder.build();
            case START_ARRAY:
                final JsonArrayBuilder arrayBuilder = provider.createArrayBuilder();
                parseArray(arrayBuilder);
                return arrayBuilder.build();
            default:
                throw new JsonParsingException("Unknown structure: " + parser.next(), parser.getLocation());
        }

    }

    public void close() {
        if (!closed) {
            closed = true;
            parser.close();
        }
    }

    private void parseObject(final JsonObjectBuilder builder) {
        String key = null;
        while (parser.hasNext()) {
            final JsonParser.Event next = parser.next();
            switch (next) {
                case KEY_NAME:
                    key = parser.getString();
                    break;

                case VALUE_STRING:
                    builder.add(key, parser.getString());
                    break;

                case START_OBJECT:
                    final JsonObjectBuilder subObject = provider.createObjectBuilder();
                    parseObject(subObject);
                    builder.add(key, subObject);
                    break;

                case START_ARRAY:
                    final JsonArrayBuilder subArray = provider.createArrayBuilder();
                    parseArray(subArray);
                    builder.add(key, subArray);
                    break;

                case VALUE_NUMBER:
                    if (parser.isIntegralNumber()) {
                        builder.add(key, parser.getLong());
                    } else {
                        builder.add(key, parser.getBigDecimal());
                    }
                    break;

                case VALUE_NULL:
                    builder.addNull(key);
                    break;

                case VALUE_TRUE:
                    builder.add(key, true);
                    break;

                case VALUE_FALSE:
                    builder.add(key, false);
                    break;

                case END_OBJECT:
                    return;

                case END_ARRAY:
                    throw new JsonParsingException("']', shouldn't occur", parser.getLocation());

                default:
                    throw new JsonParsingException(next.name() + ", shouldn't occur", parser.getLocation());
            }
        }
    }

    private void parseArray(final JsonArrayBuilder builder) {
        while (parser.hasNext()) {
            final JsonParser.Event next = parser.next();
            switch (next) {
                case VALUE_STRING:
                    builder.add(parser.getString());
                    break;

                case VALUE_NUMBER:
                    if (parser.isIntegralNumber()) {
                        builder.add(parser.getLong());
                    } else {
                        builder.add(parser.getBigDecimal());
                    }
                    break;

                case START_OBJECT:
                    JsonObjectBuilder subObject = provider.createObjectBuilder();
                    parseObject(subObject);
                    builder.add(subObject);
                    break;

                case START_ARRAY:
                    JsonArrayBuilder subArray = provider.createArrayBuilder();
                    parseArray(subArray);
                    builder.add(subArray);
                    break;

                case END_ARRAY:
                    return;

                case VALUE_NULL:
                    builder.addNull();
                    break;

                case VALUE_TRUE:
                    builder.add(true);
                    break;

                case VALUE_FALSE:
                    builder.add(false);
                    break;

                case KEY_NAME:
                    throw new JsonParsingException("array doesn't have keys", parser.getLocation());

                case END_OBJECT:
                    throw new JsonParsingException("'}', shouldn't occur", parser.getLocation());

                default:
                    throw new JsonParsingException(next.name() + ", shouldn't occur", parser.getLocation());
            }
        }
    }
}
