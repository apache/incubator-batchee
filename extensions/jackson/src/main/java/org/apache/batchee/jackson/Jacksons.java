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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public final class Jacksons {
    public static ObjectMapper newMapper(final String config) {
        final ObjectMapper mapper = new ObjectMapper();
        if (config != null) {
            final String deserializationName = DeserializationFeature.class.getSimpleName();
            final String serializationName = SerializationFeature.class.getSimpleName();
            final String mapperName = MapperFeature.class.getSimpleName();
            for (final String conf : config.split(",")) {
                final String[] parts = conf.split("=");
                parts[0] = parts[0].trim();
                parts[1] = parts[1].trim();

                if (parts[0].startsWith(deserializationName)) {
                    mapper.configure(DeserializationFeature.valueOf(parts[0].substring(deserializationName.length() + 1)), Boolean.parseBoolean(parts[1]));
                } else if (parts[0].startsWith(serializationName)) {
                    mapper.configure(SerializationFeature.valueOf(parts[0].substring(serializationName.length() + 1)), Boolean.parseBoolean(parts[1]));
                } else if (parts[0].startsWith(mapperName)) {
                    mapper.configure(MapperFeature.valueOf(parts[0].substring(mapperName.length() + 1)), Boolean.parseBoolean(parts[1]));
                } else {
                    throw new IllegalArgumentException("Ignored config: " + conf);
                }
            }
        }
        return mapper;
    }

    private Jacksons() {
        // no-op
    }
}
