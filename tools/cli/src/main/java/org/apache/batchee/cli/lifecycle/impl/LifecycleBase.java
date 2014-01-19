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
package org.apache.batchee.cli.lifecycle.impl;

import org.apache.batchee.cli.lifecycle.Lifecycle;
import org.apache.batchee.container.exception.BatchContainerRuntimeException;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public abstract class LifecycleBase<T> implements Lifecycle<T> {
    protected static final String LIFECYCLE_PROPERTIES_FILE = "batchee-lifecycle.properties";

    protected Map<String, Object> configuration(final String name) {
        final String propPath = System.getProperty("batchee.lifecycle." + name + ".properties-path", LIFECYCLE_PROPERTIES_FILE);

        final Properties p = new Properties();
        final File f = new File(propPath);
        if (f.isFile()) {
            try {
                final InputStream is = new FileInputStream(f);
                try {
                    p.load(is);
                } finally {
                    is.close();
                }
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(e);
            }
        }

        return Map.class.cast(p);
    }
}
