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
package org.apache.batchee.jaxrs.common;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RestProperties {
    private List<RestEntry> entries = new LinkedList<RestEntry>();

    public void setEntries(final List<RestEntry> entries) {
        this.entries = entries;
    }

    public List<RestEntry> getEntries() {
        return entries;
    }

    public static RestProperties wrap(final Properties parameters) {
        final RestProperties p = new RestProperties();
        for (final Map.Entry<Object, Object> e : parameters.entrySet()) {
            final RestEntry restEntry = new RestEntry();
            restEntry.setKey(e.getKey().toString());
            if (e.getValue() != null) {
                restEntry.setValue(e.getValue().toString());
            }
            p.getEntries().add(restEntry);
        }
        return p;
    }

    public static Properties unwrap(final RestProperties jobParameters) {
        final Properties p = new Properties();
        for (final RestEntry e : jobParameters.getEntries()) {
            p.setProperty(e.getKey(), e.getValue());
        }
        return p;
    }

    public static Properties toProperties(final RestProperties props) {
        final Properties p = new Properties();
        if (props != null && props.getEntries() != null) {
            for (final RestEntry entry : props.getEntries()) {
                p.put(entry.getKey(), entry.getValue());
            }
        }
        return p;
    }
}
