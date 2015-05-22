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

package org.apache.batchee.container.cdi;

import javax.batch.operations.BatchRuntimeException;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.BeforeShutdown;
import javax.enterprise.inject.spi.Extension;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// excepted beforeBeanDiscovery() all is forked from DeltaSpike - we don't want to depend from it here
public class BatchCDIInjectionExtension implements Extension {

    private static final boolean CDI_1_1_AVAILABLE;
    private static final Method CDI_CURRENT_METHOD;
    private static final Method CDI_GET_BEAN_MANAGER_METHOD;


    private static BatchCDIInjectionExtension bmpSingleton = null;

    private volatile Map<ClassLoader, BeanManagerInfo> bmInfos = new ConcurrentHashMap<ClassLoader, BeanManagerInfo>();


    static {
        boolean cdi11Available;
        Method currentMethod;
        Method getBmMethod;

        try {
            Class<?> cdi = Class.forName("javax.enterprise.inject.spi.CDI", false, loader());
            currentMethod = cdi.getDeclaredMethod("current");
            getBmMethod = cdi.getDeclaredMethod("getBeanManager");
            cdi11Available = true;
        } catch (Exception e) {
            currentMethod = null;
            getBmMethod = null;
            cdi11Available = false;
        }

        CDI_CURRENT_METHOD = currentMethod;
        CDI_GET_BEAN_MANAGER_METHOD = getBmMethod;
        CDI_1_1_AVAILABLE = cdi11Available;
    }

    void beforeBeanDiscovery(final @Observes BeforeBeanDiscovery bbd, BeanManager bm) {
        bbd.addAnnotatedType(bm.createAnnotatedType(BatchProducerBean.class));
    }

    public void setBeanManager(final @Observes AfterBeanDiscovery afterBeanDiscovery, final BeanManager beanManager) {
        // bean manager holder
        if (bmpSingleton == null) {
            bmpSingleton = this;
        }

        if (!CDI_1_1_AVAILABLE) {
            final BeanManagerInfo bmi = getBeanManagerInfo(loader());
            bmi.loadTimeBm = beanManager;
        }
    }

    public void cleanupFinalBeanManagers(final @Observes AfterDeploymentValidation adv) {
        if (!CDI_1_1_AVAILABLE) {
            for (final BeanManagerInfo bmi : bmpSingleton.bmInfos.values()) {
                bmi.finalBm = null;
            }
        }
    }

    public void cleanupStoredBeanManagerOnShutdown(final @Observes BeforeShutdown beforeShutdown) {
        if (CDI_1_1_AVAILABLE || bmpSingleton == null) {
            return;
        }

        bmpSingleton.bmInfos.remove(loader());
    }

    private static ClassLoader loader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public static BatchCDIInjectionExtension getInstance() {
        return bmpSingleton;
    }

    public BeanManager getBeanManager() {
        if (CDI_1_1_AVAILABLE) {
            try {
                return (BeanManager) CDI_GET_BEAN_MANAGER_METHOD.invoke(CDI_CURRENT_METHOD.invoke(null));
            } catch (Exception e) {
                throw new BatchRuntimeException("unable to resolve BeanManager");
            }
        }

        // fallback if CDI isn't available
        final BeanManagerInfo bmi = getBeanManagerInfo(loader());

        BeanManager result = bmi.finalBm;
        if (result == null && bmi.cdi == null) {
            synchronized (this) {
                result = resolveBeanManagerViaJndi();
                if (result == null) {
                    result = bmi.loadTimeBm;
                }
                if (result == null) {
                    bmi.cdi = false;
                    return null;
                }
                bmi.cdi = true;
                bmi.finalBm = result;
            }
        }

        return result;
    }

    private static BeanManager resolveBeanManagerViaJndi() {
        try {
            return BeanManager.class.cast(new InitialContext().lookup("java:comp/BeanManager"));
        } catch (final NamingException e) {
            return null;
        }
    }

    private BeanManagerInfo getBeanManagerInfo(final ClassLoader cl) {
        BeanManagerInfo bmi = bmpSingleton.bmInfos.get(cl);
        if (bmi == null) {
            synchronized (this) {
                for (final ClassLoader key : bmpSingleton.bmInfos.keySet()) {
                    if (key.getParent() == cl) {
                        return bmpSingleton.bmInfos.get(key);
                    }
                }

                bmi = bmpSingleton.bmInfos.get(cl);
                if (bmi == null) {
                    bmi = new BeanManagerInfo();
                    bmpSingleton.bmInfos.put(cl, bmi);
                }
            }
        }
        return bmi;
    }

    private static class BeanManagerInfo {
        private BeanManager loadTimeBm = null;
        private BeanManager finalBm = null;
        private Boolean cdi = null;
    }
}
