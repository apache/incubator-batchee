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

import net.sf.jsefa.Serializer;
import net.sf.jsefa.csv.CsvIOFactory;
import net.sf.jsefa.csv.CsvSerializer;
import net.sf.jsefa.csv.lowlevel.config.CsvLowLevelConfiguration;
import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import java.io.Serializable;

@Documentation("Writes a CSV file using JSefa.")
public class JSefaCsvWriter extends JSefaWriter {
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
    @Documentation("the header for the file")
    private String header;

    @Inject
    @BatchProperty
    @Documentation("Should the header be calculated from @Header or fieldnames if @Header is not present on fields annotated with @CsvField. " +
                   "This property will be ignored if the header property is set.")
    private Boolean writeHeader;

    @Override
    protected Serializer createSerializer() throws Exception {
        return CsvIOFactory.createFactory(
            JsefaConfigurations.newCsvConfiguration(
                defaultNoValueString, defaultQuoteMode, fieldDelimiter, lineBreak, quoteCharacter,
                quoteCharacterEscapeMode, useDelimiterAfterLastField, validationMode, validationProvider,
                lineFilter, lowLevelConfiguration, lineFilterLimit, objectAccessorProvider,
                specialRecordDelimiter, simpleTypeProvider, typeMappingRegistry),
            JsefaConfigurations.createObjectTypes(objectTypes))
            .createSerializer();
    }

    @Override
    public void open(Serializable checkpoint) throws Exception {
        super.open(checkpoint);

        // write header only on first run
        if (checkpoint != null) {
            return;
        }

        char delimiter;
        if (fieldDelimiter != null && !fieldDelimiter.isEmpty()) {
            delimiter = fieldDelimiter.charAt(0);
        } else {
            delimiter = CsvLowLevelConfiguration.Defaults.DEFAULT_FIELD_DELIMITER;
        }

        //X TODO align behavoir of DefaultBAtchArtifactFactory and CDIBatchArtifactFactory?
        //X      DefaultBatchArtifactFactory only resolves properties which are set, while
        //X      CDIBatchArtifactFactory resolves every field...
        if (header == null && writeHeader != null && writeHeader) {
            Class<?>[] classes = JsefaConfigurations.createObjectTypes(objectTypes);

            StringBuilder headerBuilder = new StringBuilder(50);
            for (JSefaCsvMapping mapping : JSefaCsvMapping.forTypes(classes)) {

                for (String headerPart : mapping.getHeader()) {

                    if (headerBuilder.length() > 0) {
                        headerBuilder.append(delimiter);
                    }

                    headerBuilder.append(headerPart);
                }
            }

            header = headerBuilder.toString();
        }

        if (header != null && !header.trim().isEmpty()) {
            ((CsvSerializer) serializer).getLowLevelSerializer().writeLine(header.trim());
        }
    }
}
