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
package org.apache.batchee.container.services.transaction;

import org.apache.batchee.container.exception.TransactionManagementException;
import org.apache.batchee.spi.TransactionManagerAdapter;

import javax.naming.InitialContext;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

public class JTAUserTransactionAdapter implements TransactionManagerAdapter {
    private static final String [] JNDI_LOCS = new String[] { // taken from OpenJPA ManagedRuntime
        "java:comp/TransactionManager", // generic, TomEE
        "javax.transaction.TransactionManager", // weblogic
        "java:/TransactionManager", // jboss, jrun, Geronimo
        "java:/DefaultDomain/TransactionManager", // jrun too
        "java:comp/pm/TransactionManager", // orion & oracle
        "java:appserver/TransactionManager", // GlassFish
        "java:pm/TransactionManager", // borland
        "aries:services/javax.transaction.TransactionManager", // Apache Aries
    };
    private static final String [] METHODS = new String[] {  // taken from OpenJPA ManagedRuntime
        "org.openejb.OpenEJB.getTransactionManager",
        "com.arjuna.jta.JTA_TransactionManager.transactionManager", // hp
        "com.bluestone.jta.SaTransactionManagerFactory.SaGetTransactionManager",
        "com.sun.jts.jta.TransactionManagerImpl.getTransactionManagerImpl",
        "com.inprise.visitransact.jta.TransactionManagerImpl."
            + "getTransactionManagerImpl", // borland
    };

    protected TransactionManager mgr = null;

    public JTAUserTransactionAdapter() {
        for (final String JNDI_LOC : JNDI_LOCS) {
            try {
                mgr = TransactionManager.class.cast(new InitialContext().lookup(JNDI_LOC));
            } catch (final Throwable t) {
                // no-op
            }
            if (mgr != null) {
                break;
            }
        }
        if (mgr == null) {
            for (final String METHOD : METHODS) {
                final String clazz = METHOD.substring(0, METHOD.lastIndexOf('.'));
                final String methodName = METHOD.substring(METHOD.lastIndexOf('.') + 1);
                try {
                    mgr = TransactionManager.class.cast(
                        Thread.currentThread().getContextClassLoader().loadClass(clazz).getMethod(methodName).invoke(null));
                } catch (final Throwable e) {
                    // no-op
                }
            }
        }
        if (mgr == null) {
            throw new TransactionManagementException("no transaction manager found");
        }
    }

    @Override
    public void begin() throws TransactionManagementException {
        try {
            mgr.begin();
        } catch (final NotSupportedException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void commit() throws TransactionManagementException {
        try {
            mgr.commit();
        } catch (final SecurityException e) {
            throw new TransactionManagementException(e);
        } catch (final IllegalStateException e) {
            throw new TransactionManagementException(e);
        } catch (final RollbackException e) {
            throw new TransactionManagementException(e);
        } catch (final HeuristicMixedException e) {
            throw new TransactionManagementException(e);
        } catch (final HeuristicRollbackException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void rollback() throws TransactionManagementException {
        try {
            mgr.rollback();
        } catch (final IllegalStateException e) {
            throw new TransactionManagementException(e);
        } catch (final SecurityException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public int getStatus() throws TransactionManagementException {
        try {
            return mgr.getStatus();
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void setRollbackOnly() throws TransactionManagementException {
        try {
            mgr.setRollbackOnly();
        } catch (final IllegalStateException e) {
            throw new TransactionManagementException(e);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }

    @Override
    public void setTransactionTimeout(final int seconds) throws TransactionManagementException {
        try {
            mgr.setTransactionTimeout(seconds);
        } catch (final SystemException e) {
            throw new TransactionManagementException(e);
        }
    }
}
