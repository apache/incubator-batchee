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
import net.sf.jsefa.flr.FlrIOFactory;
import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;

@Documentation("Reads a FLR file using JSefa.")
public class JSefaFlrReader extends JSefaReader {
    @Inject
    @BatchProperty
    @Documentation("Which ref or implementation to use to filter lines")
    private String lineFilter;

    @Inject
    @BatchProperty
    @Documentation("low level configuration implementation")
    private String lowLevelConfiguration;

    @Inject
    @BatchProperty
    @Documentation("if not using a custom line filter how many lines to filter")
    private String lineFilterLimit;

    @Inject
    @BatchProperty
    @Documentation("record delimiter")
    private String specialRecordDelimiter;

    @Inject
    @BatchProperty
    @Documentation("EOL")
    private String lineBreak;

    @Inject
    @BatchProperty
    @Documentation("pad character")
    private String defaultPadCharacter;

    @Override
    protected Deserializer initDeserializer() throws Exception {
        return FlrIOFactory.createFactory(
            JsefaConfigurations.newFlrConfiguration(defaultPadCharacter, lineBreak, validationMode, validationProvider,
                lineFilter, lowLevelConfiguration, lineFilterLimit, objectAccessorProvider, specialRecordDelimiter,
                simpleTypeProvider, typeMappingRegistry),
            JsefaConfigurations.createObjectTypes(objectTypes))
            .createDeserializer();
    }
}
