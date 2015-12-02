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

import org.apache.batchee.cli.command.Abandon;
import org.apache.batchee.cli.command.CliConfiguration;
import org.apache.batchee.cli.command.Eviction;
import org.apache.batchee.cli.command.Executions;
import org.apache.batchee.cli.command.api.Exit;
import org.apache.batchee.cli.command.Instances;
import org.apache.batchee.cli.command.Names;
import org.apache.batchee.cli.command.Restart;
import org.apache.batchee.cli.command.Running;
import org.apache.batchee.cli.command.Start;
import org.apache.batchee.cli.command.Status;
import org.apache.batchee.cli.command.Stop;
import org.apache.batchee.cli.command.api.Command;
import org.apache.batchee.cli.command.api.UserCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.Arrays.asList;

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

        final Map<String, Class<? extends Runnable>> commands = new TreeMap<String, Class<? extends Runnable>>();
        if (cliConfiguration.addDefaultCommands()) {
            for (final Class<? extends Runnable> type :
                Arrays.asList(Names.class,
                    Start.class, Restart.class,
                    Status.class, Running.class,
                    Stop.class, Abandon.class,
                    Instances.class, Executions.class,
                    Eviction.class)) {
                addCommand(commands, type);
            }
        }
        final Iterator<Class<? extends UserCommand>> userCommands = cliConfiguration.userCommands();
        if (userCommands != null) {
            while (userCommands.hasNext()) {
                addCommand(commands, userCommands.next());
            }
        }

        if (args == null || args.length == 0) {
            System.err.print(help(commands));
            return;
        }

        final Class<? extends Runnable> cmd = commands.get(args[0]);
        if (cmd == null) {
            if (args[0].equals("help")) {
                if (args.length > 1) {
                    final Class<? extends Runnable> helpCmd = commands.get(args[1]);
                    if (helpCmd != null) {
                        printHelp(helpCmd.getAnnotation(Command.class), buildOptions(helpCmd, new HashMap<String, Field>()));
                        return;
                    }
                } // else let use the default help
            }
            System.err.print(help(commands));
            return;
        }

        // build the command now
        final Command command = cmd.getAnnotation(Command.class);
        if (command == null) {
            System.err.print(help(commands));
            return;
        }

        final Map<String, Field> fields = new HashMap<String, Field>();
        final Options options = buildOptions(cmd, fields);

        final Collection<String> newArgs = new ArrayList<String>(asList(args));
        newArgs.remove(newArgs.iterator().next());

        final CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine line = parser.parse(options, newArgs.toArray(new String[newArgs.size()]));

            final Runnable commandInstance = cmd.newInstance();
            if (!newArgs.isEmpty()) { // we have few commands we can execute without args even if we have a bunch of config
                for (final Map.Entry<String, Field> option : fields.entrySet()) {
                    final String key = option.getKey();
                    if (key.isEmpty()) { // arguments, not an option
                        final List<String> list = line.getArgList();
                        if (list != null) {
                            final Field field = option.getValue();
                            final Type expectedType = field.getGenericType();
                            if (ParameterizedType.class.isInstance(expectedType)) {
                                final ParameterizedType pt = ParameterizedType.class.cast(expectedType);
                                if ((pt.getRawType() == List.class || pt.getRawType() == Collection.class)
                                    && pt.getActualTypeArguments().length == 1 && pt.getActualTypeArguments()[0] == String.class) {
                                    field.set(commandInstance, list);
                                } else {
                                    throw new IllegalArgumentException("@Arguments only supports List<String>");
                                }
                            } else {
                                throw new IllegalArgumentException("@Arguments only supports List<String>");
                            }
                        }
                    } else {
                        final String value = line.getOptionValue(key);
                        if (value != null) {
                            final Field field = option.getValue();
                            final Class<?> expectedType = field.getType();
                            if (String.class == expectedType) {
                                field.set(commandInstance, value);
                            } else if (long.class == expectedType) {
                                field.set(commandInstance, Long.parseLong(value));
                            } else if (int.class == expectedType) {
                                field.set(commandInstance, Integer.parseInt(value));
                            } else if (boolean.class == expectedType) {
                                field.set(commandInstance, Boolean.parseBoolean(value));
                            } else if (short.class == expectedType) {
                                field.set(commandInstance, Short.parseShort(value));
                            } else if (byte.class == expectedType) {
                                field.set(commandInstance, Byte.parseByte(value));
                            } else {
                                try {
                                    field.set(commandInstance, expectedType.getMethod("fromString", String.class)
                                        .invoke(null, value));
                                } catch (final Exception e) {
                                    throw new IllegalArgumentException(expectedType + " not supported as option with value '" + value + "'");
                                }
                            }
                        }
                    }
                }
            }
            cliConfiguration.decorate(commandInstance).run();
        } catch (final ParseException e) {
            printHelp(command, options);
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
        } catch (final InstantiationException e) {
            throw new IllegalStateException(e);
        } catch (final IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void printHelp(final Command command, final Options options) {
        new HelpFormatter().printHelp(command.name(), '\n' + command.description() + "\n\n", options, null, true);
    }

    private static Options buildOptions(final Class<? extends Runnable> cmd, Map<String, Field> fields) {
        final Options options = new Options();
        Class<?> it = cmd;
        while (it != null) {
            for (final Field f : it.getDeclaredFields()) {
                final org.apache.batchee.cli.command.api.Option option = f.getAnnotation(org.apache.batchee.cli.command.api.Option.class);
                final org.apache.batchee.cli.command.api.Arguments arguments = f.getAnnotation(org.apache.batchee.cli.command.api.Arguments.class);
                if (option != null && arguments != null) {
                    throw new IllegalArgumentException("An @Option can't get @Arguments: " + f);
                }

                if (option != null) {
                    final String name = option.name();
                    options.addOption(Option.builder(name).desc(option.description()).hasArg().build());
                    fields.put(name, f);
                    f.setAccessible(true);
                } else if (arguments != null) {
                    if (fields.put("", f) != null) {
                        throw new IllegalArgumentException("A command can only have a single @Arguments");
                    }
                    f.setAccessible(true);
                }
            }
            it = it.getSuperclass();
        }
        return options;
    }

    private static String help(final Map<String, Class<? extends Runnable>> commands) {
        final StringBuilder builder = new StringBuilder();
        for (final Map.Entry<String, Class<? extends Runnable>> cmd : commands.entrySet()) {
            final Command c = cmd.getValue().getAnnotation(Command.class);
            if (c == null) {
                throw new IllegalArgumentException("Missing @Command on " + cmd.getValue());
            }
            builder.append(c.name());
            if (!c.description().isEmpty()) {
                builder.append(": ").append(c.description());
            }
            builder.append(System.getProperty("line.separator"));
        }
        return builder.toString();
    }

    private static void addCommand(final Map<String, Class<? extends Runnable>> commands, final Class<? extends Runnable> type) {
        final Command command = type.getAnnotation(Command.class);
        if (command == null) {
            throw new IllegalArgumentException(type + " is not a command, missing @Command");
        }

        final String name = command.name();
        commands.put(name, type);
    }

    private BatchEECLI() {
        // no-op
    }
}
