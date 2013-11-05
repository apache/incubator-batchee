/*
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
package org.apache.batchee.container.services.factory;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.util.DependencyInjections;
import org.apache.batchee.spi.BatchArtifactFactory;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DefaultBatchArtifactFactory implements BatchArtifactFactory, XMLStreamConstants {
    private final static String BATCH_XML = "META-INF/batch.xml";
    private final static String BATCHEE_XML = "META-INF/batchee.xml"; // used for out extensions to get short names, spec doesn't impose to read multiple batch.xml so using it as a workaround
    private final static QName BATCH_ROOT_ELEM = new QName("http://xmlns.jcp.org/xml/ns/javaee", "batch-artifacts");

    // Uses TCCL
    @Override
    public Instance load(final String batchId) {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        final ArtifactMap artifactMap = createArtifactsMap(tccl);

        Object loadedArtifact = artifactMap.getArtifactById(batchId);
        if (loadedArtifact == null) {
            try {
                final Class<?> artifactClass = tccl.loadClass(batchId);
                if (artifactClass != null) {
                    loadedArtifact = artifactClass.newInstance();
                }
            } catch (final ClassNotFoundException e) {
                throw new BatchContainerRuntimeException("Tried but failed to load artifact with id: " + batchId, e);
            } catch (final InstantiationException e) {
                throw new BatchContainerRuntimeException("Tried but failed to load artifact with id: " + batchId, e);
            } catch (final IllegalAccessException e) {
                throw new BatchContainerRuntimeException("Tried but failed to load artifact with id: " + batchId, e);
            }
        }

        if (ProxyFactory.getInjectionReferences() != null) {
            DependencyInjections.injectReferences(loadedArtifact, ProxyFactory.getInjectionReferences());
        }

        return new Instance(loadedArtifact, null);
    }

    private ArtifactMap createArtifactsMap(final ClassLoader tccl) {
        final ArtifactMap artifactMap = new ArtifactMap();
        initArtifactMapFromClassLoader(artifactMap, tccl, BATCH_XML);
        initArtifactMapFromClassLoader(artifactMap, tccl, BATCHEE_XML);
        return artifactMap;
    }

    private ArtifactMap initArtifactMapFromClassLoader(final ArtifactMap map, final ClassLoader loader, final String name) {
        final Enumeration<URL> urls;
        try {
            urls = loader.getResources(name);
        } catch (final IOException e) {
            // try it as fallback
            final InputStream is = loader.getResourceAsStream(name);
            if (is == null) {
                return null;
            }
            populateArtifactMapFromStream(map, is);
            return map;
        }

        final Collection<URL> parsedUrls = new LinkedList<URL>();
        while (urls.hasMoreElements()) {
            final URL url = urls.nextElement();
            if (parsedUrls.contains(url)) { // can happen in weird classloaders graphs
                continue;
            }
            parsedUrls.add(url);

            final InputStream is;
            try {
                is = url.openStream();
            } catch (final IOException e) {
                throw new BatchContainerRuntimeException(e);
            }

            populateArtifactMapFromStream(map, is);
        }
        return map;
    }

    protected void populateArtifactMapFromStream(final ArtifactMap tempMap, final InputStream is) {
        final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try {
            final XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(is);

            boolean processedRoot = false;

            // We are going to take advantage of the simplified structure of a
            // line
            // E.g.:
            // <batch-artifacts>
            //   <item-processor id=MyItemProcessor class=jsr352/sample/MyItemProcessorImpl/>
            //   ..
            // </batch-artifacts>
            //
            // and have much simpler logic than general-purpose parsing would
            // require.
            while (xmlStreamReader.hasNext()) {
                int event = xmlStreamReader.next();

                // Until we reach end of document
                if (event == END_DOCUMENT) {
                    break;
                }

                // At this point we have either:
                //    A) just passed START_DOCUMENT, and are at START_ELEMENT for the root,
                //       <batch-artifacts>, or
                //    B) we have just passed END_ELEMENT for one of the artifacts which is a child of
                //       <batch-artifacts>.
                //
                //  Only handle START_ELEMENT now so we can skip whitespace CHARACTERS events.
                //
                if (event == START_ELEMENT) {
                    if (!processedRoot) {
                        QName rootQName = xmlStreamReader.getName();
                        if (!rootQName.equals(BATCH_ROOT_ELEM)) {
                            throw new IllegalStateException("Expecting document with root element QName: " + BATCH_ROOT_ELEM
                                + ", but found root element with QName: " + rootQName);
                        } else {
                            processedRoot = true;
                        }
                    } else {

                        // Should only need localName
                        final String annotationShortName = xmlStreamReader.getLocalName();
                        final String id = xmlStreamReader.getAttributeValue(null, "id");
                        final String className = xmlStreamReader.getAttributeValue(null, "class");
                        tempMap.addEntry(annotationShortName, id, className);

                        // Ignore anything else (text/whitespace) within this
                        // element
                        while (event != END_ELEMENT) {
                            event = xmlStreamReader.next();
                        }
                    }
                }
            }
            xmlStreamReader.close();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                is.close();
            } catch (final IOException e) {
                // no-op
            }
        }
    }

    private class ArtifactMap {
        private Map<String, Class<?>> idToArtifactClassMap = new HashMap<String, Class<?>>();

        // Maps to a list of types not a single type since there's no reason a single artifact couldn't be annotated
        // with >1 batch artifact annotation type.
        private Map<String, List<String>> idToArtifactTypeListMap = new HashMap<String, List<String>>();

        /*
         * Init already synchronized, so no need to synch further
         */
        private void addEntry(final String batchTypeName, final String id, final String className) {
            try {
                final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                if (!idToArtifactClassMap.containsKey(id)) {
                    Class<?> artifactClass = contextClassLoader.loadClass(className);

                    idToArtifactClassMap.put(id, artifactClass);
                    List<String> typeList = new ArrayList<String>();
                    typeList.add(batchTypeName);
                    idToArtifactTypeListMap.put(id, typeList);
                } else {
                    final Class<?> artifactClass = contextClassLoader.loadClass(className);

                    // Already contains entry for this 'id', let's make sure it's the same Class
                    // which thus must implement >1 batch artifact "type" (i.e. contains >1 batch artifact annotation).
                    if (!idToArtifactClassMap.get(id).equals(artifactClass)) {
                        throw new IllegalArgumentException("Already loaded a different class for id = " + id);
                    }
                    List<String> typeList = idToArtifactTypeListMap.get(id);
                    typeList.add(batchTypeName);
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        private Object getArtifactById(final String id) {
            Object artifactInstance = null;

            try {
                final Class<?> clazz = idToArtifactClassMap.get(id);
                if (clazz != null) {
                    artifactInstance = (idToArtifactClassMap.get(id)).newInstance();
                }
            } catch (final IllegalAccessException e) {
                throw new BatchContainerRuntimeException("Tried but failed to load artifact with id: " + id, e);
            } catch (final InstantiationException e) {
                throw new BatchContainerRuntimeException("Tried but failed to load artifact with id: " + id, e);
            }


            return artifactInstance;
        }
    }

    @Override
    public void init(final Properties batchConfig) throws BatchContainerServiceException {
        // no-op

    }
}
