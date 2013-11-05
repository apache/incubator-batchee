/**
 * Copyright 2012 International Business Machines Corp.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.container.jsl;

import org.apache.batchee.jaxb.Batchlet;
import org.apache.batchee.jaxb.CheckpointAlgorithm;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.ExceptionClassFilter;
import org.apache.batchee.jaxb.ItemProcessor;
import org.apache.batchee.jaxb.ItemReader;
import org.apache.batchee.jaxb.ItemWriter;
import org.apache.batchee.jaxb.JSLProperties;
import org.apache.batchee.jaxb.Listener;
import org.apache.batchee.jaxb.Listeners;
import org.apache.batchee.jaxb.ObjectFactory;
import org.apache.batchee.jaxb.Property;

import java.util.Enumeration;
import java.util.Properties;

public class CloneUtility {
    private static final ObjectFactory JSL_FACTORY = new ObjectFactory();

    public static Batchlet cloneBatchlet(final Batchlet batchlet) {
        final Batchlet newBatchlet = JSL_FACTORY.createBatchlet();
        newBatchlet.setRef(batchlet.getRef());
        newBatchlet.setProperties(cloneJSLProperties(batchlet.getProperties()));
        return newBatchlet;
    }

    public static JSLProperties cloneJSLProperties(final JSLProperties jslProps) {
        if (jslProps == null) {
            return null;
        }

        final JSLProperties newJSLProps = JSL_FACTORY.createJSLProperties();
        newJSLProps.setPartition(jslProps.getPartition());

        for (final Property jslProp : jslProps.getPropertyList()) {
            final Property newProperty = JSL_FACTORY.createProperty();
            newProperty.setName(jslProp.getName());
            newProperty.setValue(jslProp.getValue());
            newJSLProps.getPropertyList().add(newProperty);
        }

        return newJSLProps;
    }

    public static Listeners cloneListeners(final Listeners listeners) {
        if (listeners == null) {
            return null;
        }

        final Listeners newListeners = JSL_FACTORY.createListeners();

        for (final Listener listener : listeners.getListenerList()) {
            final Listener newListener = JSL_FACTORY.createListener();
            newListeners.getListenerList().add(newListener);
            newListener.setRef(listener.getRef());
            newListener.setProperties(cloneJSLProperties(listener.getProperties()));
        }

        return newListeners;
    }

    public static Chunk cloneChunk(final Chunk chunk) {
        final Chunk newChunk = JSL_FACTORY.createChunk();

        newChunk.setItemCount(chunk.getItemCount());
        newChunk.setRetryLimit(chunk.getRetryLimit());
        newChunk.setSkipLimit(chunk.getSkipLimit());
        newChunk.setTimeLimit(chunk.getTimeLimit());
        newChunk.setCheckpointPolicy(chunk.getCheckpointPolicy());

        newChunk.setCheckpointAlgorithm(cloneCheckpointAlorithm(chunk.getCheckpointAlgorithm()));
        newChunk.setProcessor(cloneItemProcessor(chunk.getProcessor()));
        newChunk.setReader(cloneItemReader(chunk.getReader()));
        newChunk.setWriter(cloneItemWriter(chunk.getWriter()));
        newChunk.setNoRollbackExceptionClasses(cloneExceptionClassFilter(chunk.getNoRollbackExceptionClasses()));
        newChunk.setRetryableExceptionClasses(cloneExceptionClassFilter(chunk.getRetryableExceptionClasses()));
        newChunk.setSkippableExceptionClasses(cloneExceptionClassFilter(chunk.getSkippableExceptionClasses()));

        return newChunk;
    }

    private static CheckpointAlgorithm cloneCheckpointAlorithm(final CheckpointAlgorithm checkpointAlgorithm) {
        if (checkpointAlgorithm == null) {
            return null;
        }

        final CheckpointAlgorithm newCheckpointAlgorithm = JSL_FACTORY.createCheckpointAlgorithm();
        newCheckpointAlgorithm.setRef(checkpointAlgorithm.getRef());
        newCheckpointAlgorithm.setProperties(cloneJSLProperties(checkpointAlgorithm.getProperties()));

        return newCheckpointAlgorithm;

    }

    private static ItemProcessor cloneItemProcessor(final ItemProcessor itemProcessor) {
        if (itemProcessor == null) {
            return null;
        }

        final ItemProcessor newItemProcessor = JSL_FACTORY.createItemProcessor();
        newItemProcessor.setRef(itemProcessor.getRef());
        newItemProcessor.setProperties(cloneJSLProperties(itemProcessor.getProperties()));

        return newItemProcessor;
    }

    private static ItemReader cloneItemReader(final ItemReader itemReader) {
        if (itemReader == null) {
            return null;
        }

        final ItemReader newItemReader = JSL_FACTORY.createItemReader();
        newItemReader.setRef(itemReader.getRef());
        newItemReader.setProperties(cloneJSLProperties(itemReader.getProperties()));

        return newItemReader;
    }

    private static ItemWriter cloneItemWriter(final ItemWriter itemWriter) {
        final ItemWriter newItemWriter = JSL_FACTORY.createItemWriter();
        newItemWriter.setRef(itemWriter.getRef());
        newItemWriter.setProperties(cloneJSLProperties(itemWriter.getProperties()));

        return newItemWriter;
    }

    private static ExceptionClassFilter cloneExceptionClassFilter(final ExceptionClassFilter exceptionClassFilter) {

        if (exceptionClassFilter == null) {
            return null;
        }

        final ExceptionClassFilter newExceptionClassFilter = JSL_FACTORY.createExceptionClassFilter();

        for (final ExceptionClassFilter.Include oldInclude : exceptionClassFilter.getIncludeList()) {
            newExceptionClassFilter.getIncludeList().add(cloneExceptionClassFilterInclude(oldInclude));
        }
        for (final ExceptionClassFilter.Exclude oldExclude : exceptionClassFilter.getExcludeList()) {
            newExceptionClassFilter.getExcludeList().add(cloneExceptionClassFilterExclude(oldExclude));
        }

        return newExceptionClassFilter;

    }

    private static ExceptionClassFilter.Include cloneExceptionClassFilterInclude(final ExceptionClassFilter.Include include) {
        if (include == null) {
            return null;
        }

        final ExceptionClassFilter.Include newInclude = JSL_FACTORY.createExceptionClassFilterInclude();
        newInclude.setClazz(include.getClazz());
        return newInclude;
    }

    private static ExceptionClassFilter.Exclude cloneExceptionClassFilterExclude(final ExceptionClassFilter.Exclude exclude) {
        if (exclude == null) {
            return null;
        }

        final ExceptionClassFilter.Exclude newExclude = JSL_FACTORY.createExceptionClassFilterExclude();
        newExclude.setClazz(exclude.getClazz());
        return newExclude;
    }


    /**
     * Creates a java.util.Properties map from a com.ibm.jbatch.jsl.model.Properties
     * object.
     */
    public static Properties jslPropertiesToJavaProperties(final JSLProperties xmlProperties) {
        final Properties props = new Properties();
        for (final Property prop : xmlProperties.getPropertyList()) {
            props.setProperty(prop.getName(), prop.getValue());
        }
        return props;

    }

    /**
     * Creates a new JSLProperties list from a java.util.Properties
     * object.
     */
    public static JSLProperties javaPropsTojslProperties(final Properties javaProps) {
        final JSLProperties newJSLProps = JSL_FACTORY.createJSLProperties();
        final Enumeration<?> keySet = javaProps.propertyNames();
        while (keySet.hasMoreElements()) {
            final String key = (String) keySet.nextElement();
            final String value = javaProps.getProperty(key);

            final Property newProperty = JSL_FACTORY.createProperty();
            newProperty.setName(key);
            newProperty.setValue(value);
            newJSLProps.getPropertyList().add(newProperty);
        }
        return newJSLProps;
    }
}
