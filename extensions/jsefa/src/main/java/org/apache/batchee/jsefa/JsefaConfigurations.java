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

import org.apache.batchee.extras.lang.Langs;
import net.sf.jsefa.common.accessor.ObjectAccessorProvider;
import net.sf.jsefa.common.config.Configuration;
import net.sf.jsefa.common.config.ValidationMode;
import net.sf.jsefa.common.converter.provider.SimpleTypeConverterProvider;
import net.sf.jsefa.common.lowlevel.filter.LineFilter;
import net.sf.jsefa.common.mapping.EntryPoint;
import net.sf.jsefa.common.mapping.TypeMappingRegistry;
import net.sf.jsefa.common.validator.provider.ValidatorProvider;
import net.sf.jsefa.csv.config.CsvConfiguration;
import net.sf.jsefa.csv.lowlevel.config.EscapeMode;
import net.sf.jsefa.csv.lowlevel.config.QuoteMode;
import net.sf.jsefa.flr.config.FlrConfiguration;
import net.sf.jsefa.rbf.config.RbfConfiguration;
import net.sf.jsefa.rbf.lowlevel.config.RbfLowLevelConfiguration;
import net.sf.jsefa.xml.config.XmlConfiguration;
import net.sf.jsefa.xml.lowlevel.config.XmlLowLevelConfiguration;
import net.sf.jsefa.xml.mapping.support.XmlDataTypeDefaultNameRegistry;
import net.sf.jsefa.xml.namespace.NamespaceManager;
import net.sf.jsefa.xml.namespace.QName;

public class JsefaConfigurations {
    public static Class<?>[] createObjectTypes(final String objectTypes) throws ClassNotFoundException {
        if (objectTypes == null) {
            throw new NullPointerException("objectTypes shouldn't be null");
        }

        final String[] types = objectTypes.split(",");
        final Class<?>[] classes = new Class<?>[types.length];
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < types.length; i++) {
            classes[i] = loader.loadClass(types[i]);
        }
        return classes;
    }

    public static <A extends TypeMappingRegistry<?>, B extends EntryPoint<?, ?>> void setConfiguration(final Configuration<A, B> configuration,
                                                        final String validationMode, final String validationProvider,
                                                        final String objectAccessorProvider, final String simpleTypeProvider,
                                                        final String typeMappingRegistry) throws Exception {
        if (validationMode != null) {
            configuration.setValidationMode(ValidationMode.valueOf(validationMode));
        }
        if (objectAccessorProvider != null) {
            configuration.setObjectAccessorProvider(ObjectAccessorProvider.class.cast(
                    Thread.currentThread().getContextClassLoader().loadClass(objectAccessorProvider).newInstance()));
        }
        if (simpleTypeProvider != null) {
            configuration.setSimpleTypeConverterProvider(SimpleTypeConverterProvider.class.cast(
                    Thread.currentThread().getContextClassLoader().loadClass(simpleTypeProvider).newInstance()));
        }
        if (typeMappingRegistry != null) {
            configuration.setTypeMappingRegistry((A) Thread.currentThread().getContextClassLoader().loadClass(typeMappingRegistry).newInstance());
        }
        if (validationProvider != null) {
            configuration.setValidatorProvider(ValidatorProvider.class.cast(Thread.currentThread().getContextClassLoader().loadClass(validationProvider).newInstance()));
        }
    }

//CHECKSTYLE:OFF
    public static <A extends RbfLowLevelConfiguration> void setRbfConfiguration(final RbfConfiguration<A> configuration,
                                                                                final String validationMode, final String validationProvider,
                                                                                final String lowLevelConfiguration, final String objectAccessorProvider,
                                                                                final String lineFilter, final String lineFilterLimit,
                                                                                final String specialRecordDelimiter, final String simpleTypeProvider,
                                                                                final String typeMappingRegistry) throws Exception {
//CHECKSTYLE:ON
        setConfiguration(configuration, validationMode, validationProvider, objectAccessorProvider, simpleTypeProvider, typeMappingRegistry);
        if (lineFilter != null) {
            configuration.setLineFilter(LineFilter.class.cast(Thread.currentThread().getContextClassLoader().loadClass(lineFilter).newInstance()));
        }
        if (lowLevelConfiguration != null) {
            configuration.setLowLevelConfiguration((A) Thread.currentThread().getContextClassLoader().loadClass(lowLevelConfiguration).newInstance());
        }
        if (lineFilterLimit != null) {
            configuration.setLineFilterLimit(Integer.parseInt(lineFilterLimit));
        }
        if (specialRecordDelimiter != null) {
            configuration.setSpecialRecordDelimiter(specialRecordDelimiter.charAt(0));
        }
    }

//CHECKSTYLE:OFF
    public static XmlConfiguration newXmlConfiguration(final String lineBreak, final String dataTypeDefaultNameRegistry, final String lineIndentation,
                                            final String lowLevelConfiguration, final String namespaceManager, final String dataTypeAttributeName,
                                            final String validationMode, final String validationProvider,
                                            final String objectAccessorProvider, final String simpleTypeProvider,
                                            final String typeMappingRegistry) throws Exception {
//CHECKSTYLE:ON
        final XmlConfiguration configuration = new XmlConfiguration();
        JsefaConfigurations.setConfiguration(configuration, validationMode, validationProvider, objectAccessorProvider, simpleTypeProvider, typeMappingRegistry);
        if (lineBreak != null) {
            configuration.setLineBreak(lineBreak);
        }
        if (dataTypeDefaultNameRegistry != null) {
            configuration.setDataTypeDefaultNameRegistry(XmlDataTypeDefaultNameRegistry.class.cast(
                    Thread.currentThread().getContextClassLoader().loadClass(dataTypeDefaultNameRegistry).newInstance()));
        }
        if (lineIndentation != null) {
            configuration.setLineIndentation(lineIndentation);
        }
        if (lowLevelConfiguration != null) {
            configuration.setLowLevelConfiguration(XmlLowLevelConfiguration.class.cast(
                    Thread.currentThread().getContextClassLoader().loadClass(lowLevelConfiguration).newInstance()));
        }
        if (namespaceManager != null) {
            configuration.setNamespaceManager(NamespaceManager.class.cast(Thread.currentThread().getContextClassLoader().loadClass(namespaceManager).newInstance()));
        }
        if (dataTypeAttributeName != null) {
            configuration.setDataTypeAttributeName(QName.create(dataTypeAttributeName.substring(1, dataTypeAttributeName.indexOf("}")),
                    dataTypeAttributeName.substring(dataTypeAttributeName.indexOf("}") + 1))); // allow {xxx}yyy syntax
        }
        return configuration;
    }

//CHECKSTYLE:OFF
    public static CsvConfiguration newCsvConfiguration(final String defaultNoValueString, final String defaultQuoteMode,
                                                       final String fieldDelimiter, final String lineBreak, final String quoteCharacter,
                                                       final String quoteCharacterEscapeMode, final String useDelimiterAfterLastField,
                                                       final String validationMode, final String validationProvider,
                                                       final String lineFilter, final String lowLevelConfiguration,
                                                       final String lineFilterLimit, final String objectAccessorProvider,
                                                       final String specialRecordDelimiter, final String simpleTypeProvider,
                                                       final String typeMappingRegistry) throws Exception {
//CHECKSTYLE:ON
        final CsvConfiguration configuration = new CsvConfiguration();
        setRbfConfiguration(configuration, validationMode, validationProvider, lowLevelConfiguration, objectAccessorProvider,
            lineFilter, lineFilterLimit, specialRecordDelimiter, simpleTypeProvider, typeMappingRegistry);
        if (defaultNoValueString != null) {
            configuration.setDefaultNoValueString(defaultNoValueString);
        }
        if (defaultQuoteMode != null) {
            configuration.setDefaultQuoteMode(QuoteMode.valueOf(defaultQuoteMode));
        }
        if (fieldDelimiter != null) {
            configuration.setFieldDelimiter(fieldDelimiter.charAt(0));
        }
        if (lineBreak != null) {
            configuration.setLineBreak(Langs.repalceEscapableChars(lineBreak));
        }
        if (quoteCharacter != null) {
            configuration.setQuoteCharacter(quoteCharacter.charAt(0));
        }
        if (quoteCharacterEscapeMode != null) {
            configuration.setQuoteCharacterEscapeMode(EscapeMode.valueOf(quoteCharacterEscapeMode));
        }
        if (useDelimiterAfterLastField != null) {
            configuration.setUseDelimiterAfterLastField(Boolean.parseBoolean(useDelimiterAfterLastField));
        }
        return configuration;
    }

//CHECKSTYLE:OFF
    public static FlrConfiguration newFlrConfiguration(final String defaultPadCharacter, final String lineBreak,
                                                       final String validationMode, final String validationProvider,
                                                       final String lineFilter, final String lowLevelConfiguration,
                                                       final String lineFilterLimit, final String objectAccessorProvider,
                                                       final String specialRecordDelimiter, final String simpleTypeProvider,
                                                       final String typeMappingRegistry) throws Exception {
//CHECKSTYLE:ON
        final FlrConfiguration configuration = new FlrConfiguration();
        setRbfConfiguration(configuration, validationMode, validationProvider, lowLevelConfiguration, objectAccessorProvider,
            lineFilter, lineFilterLimit, specialRecordDelimiter, simpleTypeProvider, typeMappingRegistry);
        if (lineBreak != null) {
            configuration.setLineBreak(Langs.repalceEscapableChars(lineBreak));
        }
        if (defaultPadCharacter != null) {
            configuration.setDefaultPadCharacter(defaultPadCharacter.charAt(0));
        }
        return configuration;
    }

    private JsefaConfigurations() {
        // no-op
    }
}
