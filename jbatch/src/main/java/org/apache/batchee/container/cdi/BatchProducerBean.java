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
package org.apache.batchee.container.cdi;

import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.util.DependencyInjections;

import javax.batch.api.BatchProperty;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.management.ObjectName;
import java.io.File;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class BatchProducerBean {
    private static final int[] EMPTY_INTS = new int[0];
    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final String[] EMPTY_STRINGS = new String[0];

    @Produces
    public JobContext getJobContext() {
        if (ProxyFactory.getInjectionReferences() != null) {
            return ProxyFactory.getInjectionReferences().getJobContext();
        }
        return null;
    }

    @Produces
    public StepContext getStepContext() {
        if (ProxyFactory.getInjectionReferences() != null) {
            return ProxyFactory.getInjectionReferences().getStepContext();
        }
        return null;
    }

    @Produces
    @BatchProperty
    public String produceProperty(final InjectionPoint injectionPoint) {
        if (injectionPoint != null && ProxyFactory.getInjectionReferences() != null) {
            final BatchProperty batchPropAnnotation = injectionPoint.getAnnotated().getAnnotation(BatchProperty.class);
            final String batchPropName;
            if (batchPropAnnotation.name().equals("")) {
                batchPropName = injectionPoint.getMember().getName();
            } else {
                batchPropName = batchPropAnnotation.name();
            }

            return DependencyInjections.getPropertyValue(ProxyFactory.getInjectionReferences().getProps(), batchPropName);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public Integer produceIntProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Integer.class, Integer.class);
        }
        return 0;
    }

    @Produces
    @BatchProperty
    public Double produceDoubleProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Double.class, Double.class);
        }
        return 0.;
    }

    @Produces
    @BatchProperty
    public Float produceFloatProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Float.class, Float.class);
        }
        return 0.f;
    }

    @Produces
    @BatchProperty
    public Short produceShortProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Short.class, Short.class);
        }
        return 0;
    }

    @Produces
    @BatchProperty
    public Boolean produceBooleanProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Boolean.class, Boolean.class);
        }
        return false;
    }

    @Produces
    @BatchProperty
    public Long produceLongProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Long.class, Long.class);
        }
        return 0L;
    }

    @Produces
    @BatchProperty
    public Byte produceByteProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Byte.class, Byte.class);
        }
        return 0;
    }

    @Produces
    @BatchProperty
    public Character produceCharProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Character.class, Character.class);
        }
        return 0;
    }

    @Produces
    @BatchProperty
    public int[] produceIntArrayProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, EMPTY_INTS.getClass(), null);
        }
        return EMPTY_INTS;
    }

    @Produces
    @BatchProperty
    public double[] produceDoubleArrayProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, EMPTY_DOUBLES.getClass(), null);
        }
        return EMPTY_DOUBLES;
    }

    @Produces
    @BatchProperty
    public String[] produceStringArrayProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, EMPTY_STRINGS.getClass(), null);
        }
        return EMPTY_STRINGS;
    }

    @Produces
    @BatchProperty
    public Date produceDateProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Date.class, Date.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public Inet4Address produceIp4Property(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Inet4Address.class, Inet4Address.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public Inet6Address produceIp6Property(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Inet6Address.class, Inet6Address.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public URI produceUriProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, URI.class, URI.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public URL produceUrlProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, URL.class, URL.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public Logger produceLoggerProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Logger.class, Logger.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public Properties producePropertiesProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Properties.class, Properties.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public Class produceClassProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Class.class, Class.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public Pattern producePatternProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, Pattern.class, Pattern.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public ObjectName produceObjectNameProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, ObjectName.class, ObjectName.class);
        }
        return null;
    }

    @Produces
    @BatchProperty
    public File produceFileProperty(final InjectionPoint injectionPoint) {
        final String v = produceProperty(injectionPoint);
        if (v != null) {
            return DependencyInjections.convertTo(v, File.class, File.class);
        }
        return null;
    }
}
