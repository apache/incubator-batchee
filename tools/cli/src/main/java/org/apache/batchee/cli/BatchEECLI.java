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
package org.apache.batchee.cli;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseException;
import org.apache.batchee.cli.command.Abandon;
import org.apache.batchee.cli.command.CliConfiguration;
import org.apache.batchee.cli.command.Eviction;
import org.apache.batchee.cli.command.Executions;
import org.apache.batchee.cli.command.Exit;
import org.apache.batchee.cli.command.Instances;
import org.apache.batchee.cli.command.Names;
import org.apache.batchee.cli.command.Restart;
import org.apache.batchee.cli.command.Running;
import org.apache.batchee.cli.command.Start;
import org.apache.batchee.cli.command.Status;
import org.apache.batchee.cli.command.Stop;
import org.apache.batchee.cli.command.UserCommand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.ServiceLoader;

import static java.lang.ClassLoader.getSystemClassLoader;

public class BatchEECLI {
    public static void main(final String[] args) {
        final Iterator<CliConfiguration> configuration = ServiceLoader.load(CliConfiguration.class).iterator();
        final CliConfiguration cliConfiguration = configuration.hasNext() ? configuration.next() : new CliConfiguration() {
            @Override
            public String name() {
                return "batchee";
            }

            @Override
            public String description() {
                return "BatchEE CLI";
            }

            @Override
            public boolean addDefaultCommands() {
                return true;
            }

            @Override
            public Iterator<Class<? extends UserCommand>> userCommands() {
                final Collection<Class<? extends UserCommand>> classes = new ArrayList<Class<? extends UserCommand>>();
                try { // read manually cause we dont want to instantiate them there, so no ServiceLoader
                    final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
                    final ClassLoader loader = tccl != null ? tccl : getSystemClassLoader();
                    final Enumeration<URL> uc = loader.getResources("META-INF/services/org.apache.batchee.cli.command.UserCommand");
                    while (uc.hasMoreElements()) {
                        final URL url = uc.nextElement();
                        BufferedReader r = null;
                        try {
                            r = new BufferedReader(new InputStreamReader(url.openStream()));
                            String line;
                            while ((line = r.readLine()) != null) {
                                if (line.startsWith("#") || line.trim().isEmpty()) {
                                    continue;
                                }
                                classes.add(Class.class.cast(loader.loadClass(line.trim())));
                            }
                        } catch (final IOException ioe) {
                            throw new IllegalStateException(ioe);
                        } catch (final ClassNotFoundException cnfe) {
                            throw new IllegalArgumentException(cnfe);
                        } finally {
                            if (r != null) {
                                r.close();
                            }
                        }
                    }
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
                return classes.iterator();
            }

            @Override
            public Runnable decorate(final Runnable task) {
                return task;
            }
        };

        final Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder(cliConfiguration.name())
                .withDescription(cliConfiguration.description())
                .withDefaultCommand(Help.class);
        if (cliConfiguration.addDefaultCommands()) {
            builder.withCommands(Help.class,
                Names.class,
                Start.class, Restart.class,
                Status.class, Running.class,
                Stop.class, Abandon.class,
                Instances.class, Executions.class,
                Eviction.class);
        }
        final Iterator<Class<? extends UserCommand>> userCommands = cliConfiguration.userCommands();
        if (userCommands != null) {
            while (userCommands.hasNext()) {
                builder.withCommand(userCommands.next());
            }
        }

        final Cli<Runnable> parser = builder.build();

        try {
            cliConfiguration.decorate(parser.parse(args)).run();
        } catch (final ParseException e) {
            parser.parse("help").run();
        } catch (final RuntimeException e) {
            Class<?> current = e.getClass();
            while (current != null) {
                final Exit annotation = current.getAnnotation(Exit.class);
                if (annotation != null) {
                    System.exit(annotation.value());
                }
                current = current.getSuperclass();
            }
            throw e;
        }
    }

    private BatchEECLI() {
        // no-op
    }
}
