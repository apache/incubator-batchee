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
package org.apache.batchee.extras.locator;

import java.io.Closeable;
import java.io.IOException;

public interface BeanLocator {
    <T> LocatorInstance<T> newInstance(final Class<T> expected, final String batchId);

    public static class LocatorInstance<T> {
        private final T value;
        private final Closeable releasable;

        public LocatorInstance(final T value, final Closeable releasable) {
            this.value = value;
            this.releasable = releasable;
        }

        public T getValue() {
            return value;
        }

        public void release() {
            if (releasable != null) {
                try {
                    releasable.close();
                } catch (final IOException e) {
                    // no-op
                }
            }
        }
    }

    public static class Finder {
        public static BeanLocator get(final String name) {
            if (name == null) {
                return BatcheeLocator.INSTANCE;
            }
            try {
                return BeanLocator.class.cast(Thread.currentThread().getContextClassLoader().loadClass(name).newInstance());
            } catch (final Throwable e) {
                return BatcheeLocator.INSTANCE;
            }
        }
    }
}
