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

import org.jsefa.Deserializer;
import org.jsefa.xml.XmlIOFactory;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;

public class JSefaXmlReader extends JSefaReader {
    @Inject
    @BatchProperty
    private String lowLevelConfiguration;

    @Inject
    @BatchProperty
    private String lineBreak;

    @Inject
    @BatchProperty
    private String dataTypeDefaultNameRegistry;

    @Inject
    @BatchProperty
    private String lineIndentation;

    @Inject
    @BatchProperty
    private String namespaceManager;

    @Inject
    @BatchProperty
    private String dataTypeAttributeName;

    @Override
    protected Deserializer initDeserializer() throws Exception {
        return XmlIOFactory.createFactory(
            JsefaConfigurations.newXmlConfiguration(lineBreak, dataTypeDefaultNameRegistry, lineIndentation,
                lowLevelConfiguration, namespaceManager, dataTypeAttributeName,
                validationMode, validationProvider, objectAccessorProvider,
                simpleTypeProvider, typeMappingRegistry),
            JsefaConfigurations.createObjectTypes(objectTypes))
            .createDeserializer();
    }
}
