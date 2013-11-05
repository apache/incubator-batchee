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
package org.apache.batchee.extras.stax.util;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;

public class SAXStAXHandler extends DefaultHandler {
    private final XMLEventWriter writer;
    private final XMLEventFactory factory;

    public SAXStAXHandler(final XMLEventWriter writer, final XMLEventFactory eventFactory) {
        this.writer = writer;
        this.factory = eventFactory;
    }

    @Override
    public void startDocument() throws SAXException {
        // no-op
    }

    @Override
    public void startElement(final String uri, final String localName, final String qName, final Attributes attributes) throws SAXException {
        try {
            writer.add(factory.createStartElement(uri, localName, qName));
            for (int i = 0; i < attributes.getLength(); i++) {
                final QName attrName = new QName(attributes.getURI(i), attributes.getQName(i));
                if (!"xmlns".equals(attrName.getLocalPart()) && !"xmlns".equals(attrName.getPrefix())) {
                    writer.add(factory.createAttribute(attrName.getPrefix(), attrName.getNamespaceURI(), attrName.getLocalPart(), attributes.getValue(i)));
                }
            }
        } catch (final XMLStreamException e) {
            throw new SAXException(e);
        }
    }

    @Override
    public void characters(final char[] ch, final int start, final int length) throws SAXException {
        try {
            writer.add(factory.createCharacters(String.valueOf(ch, start, length)));
        } catch (final XMLStreamException e) {
            throw new SAXException(e);
        }
    }

    @Override
    public void ignorableWhitespace(final char ch[], final int start, final int length) throws SAXException {
        try {
            writer.add(factory.createIgnorableSpace(String.valueOf(ch, start, length)));
        } catch (final XMLStreamException e) {
            throw new SAXException(e);
        }
    }

    @Override
    public void endElement(final String uri, final String localName, final String qName) throws SAXException {
        try {
            writer.add(factory.createEndElement(uri, localName, qName));
        } catch (final XMLStreamException e) {
            throw new SAXException(e);
        }
    }

    @Override
    public void endDocument() throws SAXException {
        // no-op
    }
}
