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

import net.sf.jsefa.Deserializer;
import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.CountedReader;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;

public abstract class JSefaReader extends CountedReader {
    @Inject
    @BatchProperty
    @Documentation("object types to use")
    protected String objectTypes;

    @Inject
    @BatchProperty
    @Documentation("validation mode (AUTO, CALLBACK, NONE)")
    protected String validationMode;

    @Inject
    @BatchProperty
    @Documentation("object accessor provider implementation")
    protected String objectAccessorProvider;

    @Inject
    @BatchProperty
    @Documentation("validation provider implementation")
    protected String validationProvider;

    @Inject
    @BatchProperty
    @Documentation("simple type provider implementation")
    protected String simpleTypeProvider;

    @Inject
    @BatchProperty
    @Documentation("type mapping registry to use")
    protected String typeMappingRegistry;

    @Inject
    @BatchProperty
    @Documentation("file to read")
    protected String file;

    @Inject
    @BatchProperty
    @Documentation("the encoding to use while reading the file. E.g. \"UTF-8\". If not specified the system default encoding is being used.")
    protected String encoding;

    protected Deserializer deserializer;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        deserializer = initDeserializer();

        Charset charset;
        if (encoding != null && !encoding.isEmpty()) {
            charset = Charset.forName(encoding);
        } else {
            charset = Charset.defaultCharset();
        }

        deserializer.open(new BufferedReader(new InputStreamReader(new FileInputStream(file), charset)));
        super.open(checkpoint);
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
