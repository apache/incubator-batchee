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
package org.apache.batchee.jsefa;

import org.apache.batchee.extras.transaction.CountedReader;
import org.jsefa.Deserializer;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import java.io.FileReader;
import java.io.Serializable;

public abstract class JSefaReader extends CountedReader {
    @Inject
    @BatchProperty
    protected String objectTypes;

    @Inject
    @BatchProperty
    protected String validationMode;

    @Inject
    @BatchProperty
    protected String objectAccessorProvider;

    @Inject
    @BatchProperty
    protected String validationProvider;

    @Inject
    @BatchProperty
    protected String simpleTypeProvider;

    @Inject
    @BatchProperty
    protected String typeMappingRegistry;

    @Inject
    @BatchProperty
    protected String file;

    protected Deserializer deserializer;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        deserializer = initDeserializer();
        super.open(checkpoint);
        deserializer.open(new FileReader(file));
    }

    protected abstract Deserializer initDeserializer() throws Exception;

    @Override
    protected Object doRead() throws Exception {
        if (!deserializer.hasNext()) {
            return null;
        }
        return deserializer.next();
    }

    @Override
    public void close() throws Exception {
        if (deserializer != null) {
            deserializer.close(true);
        }
    }
}
