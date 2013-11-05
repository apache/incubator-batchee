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
package org.apache.batchee.test.tck.jndi;

import org.apache.openejb.core.LocalInitialContextFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Hashtable;
import java.util.Properties;

public class TckContextFactory implements InitialContextFactory {
    @Override
    public Context getInitialContext(final Hashtable<?, ?> environment) throws NamingException {
        final InitialContext delegate = new InitialContext(new Properties() {{
            setProperty(Context.INITIAL_CONTEXT_FACTORY, LocalInitialContextFactory.class.getName());
        }});
        return Context.class.cast(Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[]{Context.class}, new InvocationHandler() {
            @Override
            // convert jdbc/foo to openejb:Resource/jdbc/foo since jdbc/xxx is not standard - useful for ee tests
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                if ("lookup".equals(method.getName()) && String.class.isInstance(args[0]) && String.class.cast(args[0]).startsWith("jdbc")) {
                    return method.invoke(delegate, "openejb:Resource/" + args[0]);
                }
                return method.invoke(delegate, args);
            }
        }));
    }
}
