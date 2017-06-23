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
import org.apache.batchee.cli.command.Eviction;
import org.apache.batchee.cli.command.Executions;
import org.apache.batchee.cli.command.Instances;
import org.apache.batchee.cli.command.Names;
import org.apache.batchee.cli.command.Restart;
import org.apache.batchee.cli.command.Running;
import org.apache.batchee.cli.command.Start;
import org.apache.batchee.cli.command.Status;
import org.apache.batchee.cli.command.StepExecutions;
import org.apache.batchee.cli.command.Stop;
import org.apache.batchee.cli.command.api.CliConfiguration;
import org.apache.batchee.cli.command.api.Command;
import org.apache.batchee.cli.command.api.Exit;
import org.apache.batchee.cli.command.api.UserCommand;
import org.apache.batchee.cli.command.internal.DefaultCliConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.TreeMap;

import static java.util.Arrays.asList;

public class BatchEECLI {
    public static void main(final String[] args) {
        final Iterator<CliConfiguration> configuration = ServiceLoader.load(CliConfiguration.class).iterator();
        final CliConfiguration cliConfiguration = configuration.hasNext() ? configuration.next() : new DefaultCliConfiguration();

        final Map<String, Class<? extends Runnable>> commands = new TreeMap<String, Class<? extends Runnable>>();
        if (cliConfiguration.addDefaultCommands()) {
            for (final Class<? extends Runnable> type :
                Arrays.asList(Names.class,
                    Start.class, Restart.class,
                    Status.class, Running.class,
                    Stop.class, Abandon.class,
                    Instances.class, Executions.class,
                    StepExecutions.class, Eviction.class)) {
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

        final File cliConf;
        String home = System.getProperty("batchee.home");
        if (home == null) {
            final String conf = System.getProperty("batchee.cli.configuration");
            if (conf == null) {
                cliConf = null;
            } else {
                cliConf = new File(conf);
            }
        } else {
            cliConf = new File(home, "conf/batchee-cli.properties");
        }
        if (cliConf != null && cliConf.exists()) {
            final Properties properties = new Properties() {{
                Reader reader = null;
                try {
                    reader = new FileReader(cliConf);
                    load(reader);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (final IOException e) {
                            // no-op
                        }
                    }
                }
            }};
            for (final String key : properties.stringPropertyNames()) {
                if (key.startsWith("_arguments.")) { // /!\ added whatever passed values are
                    newArgs.add(properties.getProperty(key));
                } else {
                    final String opt = "-" + key;
                    if (!newArgs.contains(opt)) {
                        newArgs.add(opt);
                        newArgs.add(properties.getProperty(key));
                    }
                }
            }
        }

        final CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine line = parser.parse(options, newArgs.toArray(new String[newArgs.size()]));
            cliConfiguration.decorate(instantiate(cmd, cliConfiguration, fields, !newArgs.isEmpty(), line)).run();
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

    private static Runnable instantiate(final Class<? extends Runnable> cmd,
                                        final CliConfiguration configuration,
                                        final Map<String, Field> fields,
                                        final boolean hasArgs,
                                        final CommandLine line) throws InstantiationException, IllegalAccessException {
        final Runnable commandInstance = cmd.newInstance();
        if (hasArgs) { // we have few commands we can execute without args even if we have a bunch of config
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
                        field.set(commandInstance, configuration.coerce(value, field.getGenericType()));
                    }
                }
            }
        }
        return commandInstance;
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
                    final Option.Builder builder = Option.builder(name).desc(option.description()).hasArg();
                    if (option.required()) {
                        builder.required();
                    }
                    options.addOption(builder.build());
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
        final String ln = System.getProperty("line.separator");
        final StringBuilder builder = new StringBuilder("Available commands:").append(ln).append(ln);
        for (final Map.Entry<String, Class<? extends Runnable>> cmd : commands.entrySet()) {
            final Command c = cmd.getValue().getAnnotation(Command.class);
            if (c == null) {
                throw new IllegalArgumentException("Missing @Command on " + cmd.getValue());
            }
            builder.append(c.name());
            if (!c.description().isEmpty()) {
                builder.append(": ").append(c.description());
            }
            builder.append(ln);
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
