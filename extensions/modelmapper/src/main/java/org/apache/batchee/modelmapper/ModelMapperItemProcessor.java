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

import org.apache.batchee.doc.api.Documentation;
import org.modelmapper.ModelMapper;
import org.modelmapper.config.Configuration;
import org.modelmapper.convention.MatchingStrategies;
import org.modelmapper.spi.MatchingStrategy;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemProcessor;
import javax.inject.Inject;
import java.util.Locale;

@Documentation("uses model mapper to process the item")
public class ModelMapperItemProcessor implements ItemProcessor {
    private volatile ModelMapper mapper = null;

    @Inject
    @BatchProperty
    @Documentation("matching strategy to use (LOOSE, STANDARD, STRICT or custom implementation)")
    private String matchingStrategy;

    @Inject
    @BatchProperty
    @Documentation("target type")
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
                        final ClassLoader loader = currentLoader();
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
                    if (matchingStrategy != null) {
                        final Configuration configuration = mapper.getConfiguration();
                        try {
                            configuration.setMatchingStrategy(MatchingStrategy.class.cast(
                                    MatchingStrategies.class.getDeclaredField(matchingStrategy.toUpperCase(Locale.ENGLISH)).get(null)));
                        } catch (final Exception e) {
                            try {
                                configuration.setMatchingStrategy(MatchingStrategy.class.cast(currentLoader().loadClass(matchingStrategy)));
                            } catch (final Exception e1) {
                                if (RuntimeException.class.isInstance(e)) {
                                    throw RuntimeException.class.cast(e);
                                }
                                throw new IllegalStateException(e);
                            }
                        }
                    }
                }
            }
        }
        return mapper;
    }

    private static ClassLoader currentLoader() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = ModelMapperItemProcessor.class.getClassLoader();
        }
        return loader;
    }
}
