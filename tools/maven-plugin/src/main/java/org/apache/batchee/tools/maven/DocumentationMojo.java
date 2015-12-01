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
package org.apache.batchee.tools.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;

import java.beans.Introspector;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.objectweb.asm.ClassReader.SKIP_CODE;
import static org.objectweb.asm.ClassReader.SKIP_DEBUG;
import static org.objectweb.asm.ClassReader.SKIP_FRAMES;
import static org.objectweb.asm.Opcodes.ASM5;

@Mojo(name = "doc", defaultPhase = LifecyclePhase.PROCESS_CLASSES)
public class DocumentationMojo extends AbstractMojo {
    private static final String PROPERTY_MARKER = "Ljavax/batch/api/BatchProperty;";
    private static final String NAMED_MARKER = "Ljavax/inject/Named;";
    private static final String INJECT_MARKER = "Ljavax/inject/Inject;";
    private static final String CONFIGURATION_MARKER = "Lorg/apache/batchee/doc/api/Configuration;";

    @Parameter(property = "batchee.classes", defaultValue = "${project.build.outputDirectory}")
    protected File classes;

    @Parameter(property = "batchee.output", defaultValue = "${project.build.directory}/generated-docs/${project.artifactId}-jbatch.adoc")
    protected File output;

    @Parameter(property = "batchee.formatter", defaultValue = "adoc")
    protected String formatter;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (classes == null || !classes.isDirectory()) {
            getLog().warn((classes != null ? classes.getAbsolutePath() : "null") + " is not a directory, skipping");
            return;
        }

        // instantiate the formatter now to avoid to scan for nothing if we can't instantiate it
        final Formatter formatterInstance = createFormatter();

        // find meta
        final Map<String, Collection<FieldDoc>> configByComponent = new TreeMap<String, Collection<FieldDoc>>();
        try {
            scan(configByComponent, classes);
        } catch (final IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

        if (configByComponent.isEmpty()) {
            getLog().warn("Nothing found, maybe adjust <classes>, skipping.");
            return;
        }

        // format what we found
        if (!output.getParentFile().isDirectory() && !output.getParentFile().mkdirs()) {
            throw new MojoExecutionException("Can't create " + output.getAbsolutePath());
        }
        FileWriter writer = null;
        try {
            writer = new FileWriter(output);
            formatterInstance.begin(writer);
            for (final Map.Entry<String, Collection<FieldDoc>> component : configByComponent.entrySet()) {
                formatterInstance.beginClass(writer, component.getKey());
                for (final FieldDoc doc : component.getValue()) {
                    formatterInstance.add(writer, doc);
                }
                formatterInstance.endClass(writer);
            }
            formatterInstance.end(writer);
        } catch (final IOException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (final IOException e) {
                    // no-op
                }
            }
        }
    }

    private Formatter createFormatter() {
        if (formatter == null || formatter.startsWith("adoc")) {
            final int level = "adoc".equals(formatter) || formatter == null ? 1 : Integer.parseInt(formatter.substring("adoc".length()));
            final String prefix;
            {
                final StringBuilder builder = new StringBuilder();
                for (int i = 0; i < level; i++) {
                    builder.append("=");
                }
                prefix = builder.toString();
            }

            return new Formatter() {
                @Override
                public void begin(final Writer writer) throws IOException {
                    // no-op
                }

                @Override
                public void beginClass(final Writer writer, final String className) throws IOException {
                    writer.append(prefix).append(" ").append(className).append("\n\n|===\n|Name|Description\n");
                }

                @Override
                public void add(final Writer writer, final FieldDoc doc) throws IOException {
                    writer.append("|").append(doc.getName()).append("|").append(doc.getDoc() == null ? "-" : doc.getDoc()).append("\n");
                }

                @Override
                public void endClass(final Writer writer) throws IOException {
                    writer.append("|===\n\n");
                }

                @Override
                public void end(final Writer writer) throws IOException {
                    // no-op
                }
            };
        }
        try {
            return Formatter.class.cast(Thread.currentThread().getContextClassLoader().loadClass(formatter.trim()).newInstance());
        } catch (final InstantiationException e) {
            throw new IllegalArgumentException(e);
        } catch (final IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void scan(final Map<String, Collection<FieldDoc>> commands, final File file) throws IOException {
        if (file.isFile()) {
            if (file.getName().endsWith(".class")) {
                component(commands, file);
            } // else we don't care
        } else if (file.isDirectory()) {
            final File[] children = file.listFiles();
            if (children != null) {
                for (final File child : children) {
                    scan(commands, child);
                }
            }
        }
    }

    private String component(final Map<String, Collection<FieldDoc>> commands, final File classFile) throws IOException {
        InputStream stream = null;
        try {
            stream = new FileInputStream(classFile);
            final ClassReader reader = new ClassReader(stream);
            reader.accept(new ClassVisitor(ASM5) {
                private String className;
                private List<FieldDoc> configs;

                @Override
                public void visit(final int version, final int access, final String name, final String signature, final String superName, final String[] interfaces) {
                    className = name.replace('/', '.');
                }

                @Override
                public AnnotationVisitor visitAnnotation(final String desc, final boolean visible) {
                    final AnnotationVisitor annotationVisitor = super.visitAnnotation(desc, visible);
                    if (NAMED_MARKER.equals(desc)) {
                        final int dollar = className.lastIndexOf('$');
                        if (dollar > 0) {
                            className = className.substring(dollar + 1);
                        } else {
                            final int dot = className.lastIndexOf('.');
                            if (dot > 0) {
                                className = className.substring(dot + 1);
                            }
                        }

                        className = Introspector.decapitalize(className);
                        return new AnnotationVisitor(ASM5, annotationVisitor) {
                            @Override
                            public void visit(final String name, final Object value) {
                                super.visit(name, value);
                                if ("value".equals(name) && String.class.isInstance(value) && !String.class.cast(value).isEmpty()) {
                                    className = value.toString();
                                }
                            }
                        };
                    }
                    return annotationVisitor;
                }

                @Override
                public FieldVisitor visitField(final int access, final String name, final String desc, final String signature, final Object value) {
                    return new FieldVisitor(ASM5, super.visitField(access, name, desc, signature, value)) {
                        private boolean marked = false;
                        private boolean hasInject = false;
                        private String configName = name;
                        private String doc = null;

                        @Override
                        public AnnotationVisitor visitAnnotation(final String desc, final boolean visible) {
                            final AnnotationVisitor annotationVisitor = super.visitAnnotation(desc, visible);
                            if (PROPERTY_MARKER.equals(desc)) {
                                marked = true;
                                return new AnnotationVisitor(ASM5, annotationVisitor) {
                                    @Override
                                    public void visit(final String name, final Object value) {
                                        super.visit(name, value);
                                        if ("name".equals(name) && String.class.isInstance(value) && !String.class.cast(value).isEmpty()) {
                                            configName = value.toString();
                                        }
                                    }
                                };
                            }
                            if (INJECT_MARKER.equals(desc)) {
                                hasInject = true;
                                return annotationVisitor;
                            }
                            if (CONFIGURATION_MARKER.equals(desc)) {
                                return new AnnotationVisitor(ASM5, annotationVisitor) {
                                    @Override
                                    public void visit(final String name, final Object value) {
                                        super.visit(name, value);
                                        if ("value".equals(name) && String.class.isInstance(value) && !String.class.cast(value).isEmpty()) {
                                            doc = value.toString();
                                        }
                                    }
                                };
                            }
                            return annotationVisitor;
                        }

                        @Override
                        public void visitEnd() {
                            super.visitEnd();
                            if (marked && hasInject) {
                                if (configs == null) {
                                    configs = new ArrayList<FieldDoc>();
                                }
                                configs.add(new FieldDoc(configName, doc));
                            }
                        }
                    };
                }

                @Override
                public void visitEnd() {
                    super.visitEnd();
                    if (configs != null) {
                        Collections.sort(configs);
                        commands.put(className, configs);
                    }
                }
            }, SKIP_CODE + SKIP_DEBUG + SKIP_FRAMES);
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (final IOException e) {
                // no-op
            }
        }
        return null;
    }

    public static class FieldDoc implements Comparable<FieldDoc> {
        private final String name;
        private final String doc;

        private FieldDoc(final String name, final String doc) {
            this.name = name;
            this.doc = doc;
        }

        @Override
        public int compareTo(final FieldDoc o) {
            return name.compareTo(o.name);
        }

        public String getName() {
            return name;
        }

        public String getDoc() {
            return doc;
        }
    }

    public interface Formatter {
        void begin(Writer writer) throws IOException;

        void beginClass(Writer writer, String className) throws IOException;

        void add(Writer writer, FieldDoc doc) throws IOException;

        void endClass(Writer writer) throws IOException;

        void end(Writer writer) throws IOException;
    }
}
