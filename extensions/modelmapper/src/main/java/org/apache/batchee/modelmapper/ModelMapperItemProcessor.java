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
package org.apache.batchee.modelmapper;

import org.modelmapper.ModelMapper;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemProcessor;
import javax.inject.Inject;

public class ModelMapperItemProcessor implements ItemProcessor {
    private volatile ModelMapper mapper = null;

    @Inject
    @BatchProperty
    private String destinationType;
    private volatile Class<?> destinationTypeClass = null;

    @Override
    public Object processItem(final Object o) throws Exception {
        loadClass();
        return ensureMapper().map(o, destinationTypeClass);
    }

    protected ModelMapper newMapper() {
        return new ModelMapper();
    }

    private void loadClass() {
        if (destinationTypeClass == null) {
            if (destinationType == null) {
                throw new IllegalArgumentException("Please set destinationType");
            }
            synchronized (this) {
                if (destinationTypeClass == null) {
                    try {
                        ClassLoader loader = Thread.currentThread().getContextClassLoader();
                        if (loader == null) {
                            loader = ModelMapperItemProcessor.class.getClassLoader();
                        }

                        destinationTypeClass = loader.loadClass(destinationType);
                    } catch (final ClassNotFoundException e) {
                        throw new IllegalArgumentException("Can't load: '" + destinationType + "'", e);
                    } catch (final NoClassDefFoundError e) {
                        throw new IllegalArgumentException("Can't load: '" + destinationType + "'", e);
                    }
                }
            }
        }
    }

    private ModelMapper ensureMapper() {
        if (mapper == null) {
            synchronized (this) {
                if (mapper == null) {
                    mapper = newMapper();
                }
            }
        }
        return mapper;
    }
}
