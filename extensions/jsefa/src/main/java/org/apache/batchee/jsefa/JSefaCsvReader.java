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
import net.sf.jsefa.common.lowlevel.filter.HeaderAndFooterFilter;
import net.sf.jsefa.csv.CsvIOFactory;
import net.sf.jsefa.csv.config.CsvConfiguration;
import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;

@Documentation("Reads a CSV file using JSefa.")
public class JSefaCsvReader extends JSefaReader {
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
    @Documentation("which string to use for null values")
    private String defaultNoValueString;

    @Inject
    @BatchProperty
    @Documentation("quote mode (ALWAYS, ON_DEMAND, NEVER, DEFAULT)")
    private String defaultQuoteMode;

    @Inject
    @BatchProperty
    @Documentation("field delimiter")
    private String fieldDelimiter;

    @Inject
    @BatchProperty
    @Documentation("quote charater to use")
    private String quoteCharacter;

    @Inject
    @BatchProperty
    @Documentation("escape mode (ESCAPE_CHARACTER, DOUBLING)")
    private String quoteCharacterEscapeMode;

    @Inject
    @BatchProperty
    @Documentation("should deliimter be used after last field")
    private String useDelimiterAfterLastField;

    @Inject
    @BatchProperty
    @Documentation("should the header be ignored")
    private Boolean ignoreHeader;

    @Inject
    @BatchProperty
    @Documentation("number of header lines")
    private Integer headerSize;


    @Override
    protected Deserializer initDeserializer() throws Exception {
        CsvConfiguration config = JsefaConfigurations.newCsvConfiguration(
                defaultNoValueString, defaultQuoteMode, fieldDelimiter, lineBreak, quoteCharacter,
                quoteCharacterEscapeMode, useDelimiterAfterLastField, validationMode, validationProvider,
                lineFilter, lowLevelConfiguration, lineFilterLimit, objectAccessorProvider,
                specialRecordDelimiter, simpleTypeProvider, typeMappingRegistry);

        if (config.getLineFilter() == null &&
            Boolean.TRUE.equals(ignoreHeader)) {

            if (headerSize == null || headerSize == 0) {
                headerSize = 1; // the default size if nothing was specified
            }

            config.setLineFilter(new HeaderAndFooterFilter(headerSize, false, false));
        }

        return CsvIOFactory.createFactory(config, JsefaConfigurations.createObjectTypes(objectTypes))
                           .createDeserializer();
    }
}
