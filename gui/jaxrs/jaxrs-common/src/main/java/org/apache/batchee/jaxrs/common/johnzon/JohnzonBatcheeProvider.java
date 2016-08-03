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
package org.apache.batchee.jaxrs.common.johnzon;

import org.apache.johnzon.jaxrs.JohnzonProvider;
import org.apache.johnzon.mapper.MapperBuilder;
import org.apache.johnzon.mapper.converter.TimestampAdapter;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.ext.Provider;
import java.util.Comparator;

/**
 * Johnzon integration provider to keep old date formatting.
 *
 * Note 1: that if your client and server use the same formatting/provider (jackson, default johnzon, other...)
 * then this one is useless.
 *
 * Note 2: this provider also sort attribute in a deterministic order (String natural order).
 */
@Provider
@Produces("application/json")
@Consumes("application/json")
public class JohnzonBatcheeProvider<T> extends JohnzonProvider<T> {
    public JohnzonBatcheeProvider() {
        super(new MapperBuilder()
                .addAdapter(new TimestampAdapter()) // backward compatibility
                .setAttributeOrder(new Comparator<String>() { // deterministic order (useful when used with scripts)
                    @Override
                    public int compare(final String o1, final String o2) {
                        return o1.compareTo(o2);
                    }
                })
                .build(), null);
    }
}
