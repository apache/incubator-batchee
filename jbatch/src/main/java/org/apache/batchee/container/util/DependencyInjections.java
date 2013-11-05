/**
 * Copyright 2013 International Business Machines Corp.
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
package org.apache.batchee.container.util;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.exception.IllegalBatchPropertyException;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.jaxb.Property;

import javax.batch.api.BatchProperty;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DependencyInjections {
    public static void injectReferences(final Object artifact, final InjectionReferences injectionRefs) {
        final Map<String, Field> propertyMap = findPropertyFields(artifact);
        if (injectionRefs.getProps() != null) {
            injectProperties(artifact, injectionRefs.getProps(), propertyMap);
        }
        injectBatchContextFields(artifact, injectionRefs.getJobContext(), injectionRefs.getStepContext());
    }


    /**
     * @param props The properties directly associated with this batch artifact.
     */
    private static void injectProperties(final Object artifact, final List<Property> props, final Map<String, Field> propertyFieldMap) {

        //check if jsl properties are null or if 
        //the propertyMap is null. this means there are no annotated fields with @BatchProperty

        if (props == null || propertyFieldMap == null) {
            return;
        }

        // go through each field marked with @BatchProperty
        for (final Entry<String, Field> batchProperty : propertyFieldMap.entrySet()) {
            String propValue = getPropertyValue(props, batchProperty.getKey());

            // if a property is supplied in the job xml inject the given value
            // into
            // the field otherwise the default value will remain
            try {
                if (!(propValue == null)) {
                    batchProperty.getValue().set(artifact, propValue);
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalBatchPropertyException("The given property value is not an instance of the declared field.", e);
            } catch (IllegalAccessException e) {
                throw new BatchContainerRuntimeException(e);
            }

        }

    }


    /**
     * @param props list of properties from job xml
     * @param name  name of the property
     * @return null if no matching property found
     */
    public static String getPropertyValue(final List<Property> props, final String name) {
        if (props == null) {
            return null;
        }

        for (final Property prop : props) {
            if (name.equals(prop.getName())) {

                String propValue = prop.getValue();
                if ("".equals(propValue)) {
                    return null;
                } else {
                    return propValue;
                }

            }
        }


        return null;
    }

    /**
     * @param artifact An instance of the batch artifact
     * @return an ArrayList<Field> of fields annotated with @JobContext
     */
    private static void injectBatchContextFields(final Object artifact, final JobContext jobCtx, final StepContext stepCtx) {
        // Go through declared field annotations
        for (final Field field : artifact.getClass().getDeclaredFields()) {
            setAccessible(field);

            final Inject injectAnnotation = field.getAnnotation(Inject.class);
            if (injectAnnotation != null) {
                try {
                    // check the field for the context type
                    if (JobContext.class.isAssignableFrom(field.getType()) && field.get(artifact) == null) {
                        field.set(artifact, jobCtx);
                    } else if (StepContext.class.isAssignableFrom(field.getType()) && field.get(artifact) == null) {
                        field.set(artifact, stepCtx);
                    }
                } catch (final IllegalArgumentException e) {
                    throw new BatchContainerRuntimeException(e);
                } catch (final IllegalAccessException e) {
                    throw new BatchContainerRuntimeException(e);
                }
            }
        }
    }

    /**
     * @param delegate An instance of the batch artifact
     * @return A map of Fields annotated with @BatchProperty.
     */
    private static Map<String, Field> findPropertyFields(final Object delegate) {
        Map<String, Field> propertyMap = null;

        Class<?> current = delegate.getClass();
        while (current.getName().contains("$$")) { // remove common proxies
            current = current.getSuperclass();
        }

        while (current != null && current != Object.class) {
            for (final Field field : current.getDeclaredFields()) {
                setAccessible(field);

                final BatchProperty batchPropertyAnnotation = field.getAnnotation(BatchProperty.class);
                if (batchPropertyAnnotation != null) {
                    if (propertyMap == null) {
                        propertyMap = new HashMap<String, Field>();
                    }
                    // If a name is not supplied the batch property name defaults to
                    // the field name
                    String batchPropName = null;
                    if (batchPropertyAnnotation.name().equals("")) {
                        batchPropName = field.getName();
                    } else {
                        batchPropName = batchPropertyAnnotation.name();
                    }

                    // Check if we have already used this name for a property.
                    if (propertyMap.containsKey(batchPropName)) {
                        throw new IllegalBatchPropertyException("There is already a batch property with this name: " + batchPropName);
                    }

                    propertyMap.put(batchPropName, field);
                }

            }
            current = current.getSuperclass();
        }
        return propertyMap;
    }

    private static void setAccessible(final Field field) {
        if (System.getSecurityManager() == null) {
            field.setAccessible(true);
        } else {
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    field.setAccessible(true); // ignore java accessibility
                    return null;
                }
            });
        }
    }

}
