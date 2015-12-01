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
package org.apache.batchee.extras.stax;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.CountedReader;
import org.apache.batchee.extras.stax.util.JAXBContextFactory;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;

@Documentation("Reads XML items using StAX.")
public class StaxItemReader extends CountedReader implements ItemReader {
    @Inject
    @BatchProperty
    @Documentation("JAXB type(s) to instantiate for items, supports CSV values")
    private String marshallingClasses;

    @Inject
    @BatchProperty
    @Documentation("JAXB package if needed (optional)")
    private String marshallingPackage;

    @Inject
    @BatchProperty
    @Documentation("Tag to match - others are ignored")
    private String tag;

    @Inject
    @BatchProperty
    @Documentation("Input file path")
    private String input;

    private XMLEventReader reader;
    private Unmarshaller unmarshaller;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        if (input == null) {
            throw new BatchRuntimeException("input should be set");
        }
        if (tag == null) {
            throw new BatchRuntimeException("tag should be set");
        }
        if (marshallingPackage == null && marshallingClasses == null) {
            throw new BatchRuntimeException("marshallingPackage should be set");
        }

        unmarshaller = JAXBContextFactory.getJaxbContext(marshallingPackage, marshallingClasses).createUnmarshaller();
        final InputStream is = findInput();
        if (is == null) {
            throw new BatchRuntimeException("Can't find input '" + input + "'");
        }

        reader = XMLInputFactory.newInstance().createXMLEventReader(is);

        super.open(checkpoint);
    }

    private InputStream findInput() throws FileNotFoundException {
        final File file = new File(input);
        if (file.exists()) {
            return new FileInputStream(file);
        }
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(input);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    @Override
    protected Object doRead() throws Exception {
        XMLEvent xmlEvent;
        boolean found = false;
        while (reader.hasNext()) {
            try {
                xmlEvent = reader.peek();
                if (xmlEvent != null && xmlEvent.isStartElement() && tag.equals(xmlEvent.asStartElement().getName().getLocalPart())) {
                    found = true;
                    break;
                }
                reader.nextEvent();
            } catch (final XMLStreamException e) {
                // no-op
            }
        }
        if (!found) {
            return null;
        }

        try {
            final Object jaxbObject = unmarshaller.unmarshal(reader);

            if (JAXBElement.class.isInstance(jaxbObject)) {
                JAXBElement jbe = (JAXBElement) jaxbObject;
                return JAXBElement.class.cast(jbe).getValue();
            }
            return jaxbObject;
        } catch (final JAXBException ue) {
            throw new BatchRuntimeException(ue);
        }
    }
}
