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
package org.apache.batchee.tools.maven.doc;

import edu.uci.ics.jung.algorithms.layout.AbstractLayout;
import edu.uci.ics.jung.algorithms.layout.CircleLayout;
import edu.uci.ics.jung.algorithms.layout.FRLayout;
import edu.uci.ics.jung.algorithms.layout.KKLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.algorithms.layout.SpringLayout;
import edu.uci.ics.jung.algorithms.shortestpath.DijkstraDistance;
import edu.uci.ics.jung.algorithms.shortestpath.Distance;
import edu.uci.ics.jung.algorithms.shortestpath.UnweightedShortestPath;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.util.Context;
import edu.uci.ics.jung.graph.util.Pair;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.RenderContext;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.DefaultModalGraphMouse;
import edu.uci.ics.jung.visualization.decorators.EdgeShape;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.BasicEdgeLabelRenderer;
import edu.uci.ics.jung.visualization.renderers.Renderer;
import edu.uci.ics.jung.visualization.transform.shape.GraphicsDecorator;
import org.apache.batchee.container.jsl.ExecutionElement;
import org.apache.batchee.container.jsl.JobModelResolver;
import org.apache.batchee.container.jsl.TransitionElement;
import org.apache.batchee.container.navigator.JobNavigator;
import org.apache.batchee.jaxb.End;
import org.apache.batchee.jaxb.Fail;
import org.apache.batchee.jaxb.Flow;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.Next;
import org.apache.batchee.jaxb.Split;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.jaxb.Stop;
import org.apache.commons.collections15.Transformer;
import org.apache.commons.collections15.functors.ConstantTransformer;
import org.codehaus.plexus.util.IOUtil;

import javax.imageio.ImageIO;
import javax.swing.JFrame;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.GridLayout;
import java.awt.Paint;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class DiagramGenerator {
    protected final String path;
    protected final boolean failIfMissing;
    protected final boolean view;
    protected final int width;
    protected final int height;
    protected final boolean adjust;
    protected final File output;
    protected final String format;
    protected final String outputFileName;
    protected final boolean rotateEdges;
    protected final String layout;

//CHECKSTYLE:OFF
    public DiagramGenerator(final String path, final boolean failIfMissing,
                            final boolean view, final int width, final int height,
                            final boolean adjust, final File output, final String format,
                            final String outputFileName, final boolean rotateEdges, final String layout) {
//CHECKSTYLE:ON
        this.path = path;
        this.failIfMissing = failIfMissing;
        this.view = view;
        this.width = width;
        this.height = height;
        this.adjust = adjust;
        this.output = output;
        this.format = format;
        this.outputFileName = outputFileName;
        this.rotateEdges = rotateEdges;
        this.layout = layout;
    }

    public void execute() {
        final String content = slurp(validInput());

        final JSLJob job = new JobModelResolver().resolveModel(content);

        final List<ExecutionElement> executionElements = job.getExecutionElements();
        if (executionElements == null) {
            warn("No step found, no diagram will be generated.");
            return;
        }

        final Diagram diagram = new Diagram(job.getId());
        visitBatch(job, diagram);

        draw(diagram);
    }

    private void visitBatch(final JSLJob job, final Diagram diagram) {
        final Map<String, Node> nodes = new HashMap<String, Node>();

        String first = null;
        try {
            first = new JobNavigator(job).getFirstExecutionElement(null).getId();
        } catch (final Exception e) {
            // no-op
        }

        // create nodes
        final List<ExecutionElement> executionElements = job.getExecutionElements();
        final Collection<ExecutionElement> allElements = new HashSet<ExecutionElement>();
        initNodes(diagram, nodes, allElements, executionElements);

        // create edges
        for (final ExecutionElement element : allElements) {
            final String id = element.getId();
            final Node source = nodes.get(id);
            if (id.equals(first)) {
                source.root();
            }

            if (Step.class.isInstance(element)) {
                final String next = Step.class.cast(element).getNextFromAttribute();
                if (next != null) {
                    final Node target = addNodeIfMissing(diagram, nodes, next, Node.Type.STEP);
                    diagram.addEdge(new Edge("next"), source, target);
                }
            }

            for (final TransitionElement transitionElement : element.getTransitionElements()) {
                if (Stop.class.isInstance(transitionElement)) {
                    final Stop stop = Stop.class.cast(transitionElement);

                    final String restart = stop.getRestart();
                    if (restart != null) {
                        final Node target = addNodeIfMissing(diagram, nodes, restart, Node.Type.STEP);
                        diagram.addEdge(new Edge("stop(" + stop.getOn() + ")"), source, target);
                    }

                    final String exitStatus = stop.getRestart();
                    if (exitStatus != null) {
                        final Node target = addNodeIfMissing(diagram, nodes, exitStatus, Node.Type.SINK);
                        diagram.addEdge(new Edge("stop(" + stop.getOn() + ")"), source, target);
                    }
                } else if (Fail.class.isInstance(transitionElement)) {
                    final Fail fail = Fail.class.cast(transitionElement);
                    final String exitStatus = fail.getExitStatus();
                    final Node target = addNodeIfMissing(diagram, nodes, exitStatus, Node.Type.SINK);
                    diagram.addEdge(new Edge("fail(" + fail.getOn() + ")"), source, target);
                } else if (End.class.isInstance(transitionElement)) {
                    final End end = End.class.cast(transitionElement);
                    final String exitStatus = end.getExitStatus();
                    final Node target = addNodeIfMissing(diagram, nodes, exitStatus, Node.Type.SINK);
                    diagram.addEdge(new Edge("end(" + end.getOn() + ")"), source, target);
                } else if (Next.class.isInstance(transitionElement)) {
                    final Next end = Next.class.cast(transitionElement);
                    final String to = end.getTo();
                    final Node target = addNodeIfMissing(diagram, nodes, to, Node.Type.STEP);
                    diagram.addEdge(new Edge("next(" + end.getOn() + ")"), source, target);
                } else {
                    warn("Unknown next element: " + transitionElement);
                }
            }
        }
    }

    protected abstract void warn(String s);
    protected abstract void info(String s);

    private void initNodes(final Diagram diagram, final Map<String, Node> nodes,
                           final Collection<ExecutionElement> allElements, final Collection<ExecutionElement> executionElements) {
        for (final ExecutionElement element : executionElements) {
            final String id = element.getId();
            allElements.add(element);

            final Node node = addNodeIfMissing(diagram, nodes, id, Node.Type.STEP);

            if (Split.class.isInstance(element)) {
                final Split split = Split.class.cast(element);
                final List<Flow> flows = split.getFlows();
                for (final Flow flow : flows) {
                    initNodes(diagram, nodes, allElements, flow.getExecutionElements());
                    if (!flow.getExecutionElements().isEmpty()) {
                        final Node target = nodes.get(flow.getExecutionElements().iterator().next().getId());
                        if (target != null) {
                            diagram.addEdge(new Edge("split"), node, target);
                        }
                    }
                }
            } else if (Flow.class.isInstance(element)) {
                initNodes(diagram, nodes, allElements, Flow.class.cast(element).getExecutionElements());
            } // else if step or decision -> ok
        }
    }

    private static Node addNodeIfMissing(final Diagram diagram, final Map<String, Node> nodes, final String id, final Node.Type type) {
        Node node = nodes.get(id);
        if (node == null) {
            node = new Node(id, type);
            nodes.put(id, node);
            diagram.addVertex(node);
        }
        return node;
    }

    private void draw(final Diagram diagram) {
        final Layout<Node, Edge> diagramLayout = newLayout(diagram);

        final Dimension outputSize = new Dimension(width, height);
        final VisualizationViewer<Node, Edge> viewer = new GraphViewer(diagramLayout, rotateEdges);

        if (LevelLayout.class.isInstance(diagramLayout)) {
            LevelLayout.class.cast(diagramLayout).vertexShapeTransformer = viewer.getRenderContext().getVertexShapeTransformer();
        }

        diagramLayout.setSize(outputSize);
        diagramLayout.reset();
        viewer.setPreferredSize(diagramLayout.getSize());
        viewer.setSize(diagramLayout.getSize());

        // saving it too
        if (!output.exists() && !output.mkdirs()) {
            throw new IllegalStateException("Can't create '" + output.getPath() + "'");
        }
        saveView(diagramLayout.getSize(), outputSize, diagram.getName(), viewer);

        // viewing the window if necessary
        if (view) {
            final JFrame window = createWindow(viewer, diagram.getName());
            final CountDownLatch latch = new CountDownLatch(1);
            window.setVisible(true);
            window.addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosed(WindowEvent e) {
                    super.windowClosed(e);
                    latch.countDown();
                }
            });
            try {
                latch.await();
            } catch (final InterruptedException e) {
                warn("can't await window close event: " + e.getMessage());
            }
        }
    }

    private Layout<Node, Edge> newLayout(final Diagram diagram) {
        final Layout<Node, Edge> diagramLayout;
        if (layout != null && layout.startsWith("spring")) {
            diagramLayout = new SpringLayout<Node, Edge>(diagram, new ConstantTransformer(Integer.parseInt(config("spring", "100"))));
        } else if (layout != null && layout.startsWith("kk")) {
            Distance<Node> distance = new DijkstraDistance<Node, Edge>(diagram);
            if (layout.endsWith("unweight")) {
                distance = new UnweightedShortestPath<Node, Edge>(diagram);
            }
            diagramLayout = new KKLayout<Node, Edge>(diagram, distance);
        } else if (layout != null && layout.equalsIgnoreCase("circle")) {
            diagramLayout = new CircleLayout<Node, Edge>(diagram);
        } else if (layout != null && layout.equalsIgnoreCase("fr")) {
            diagramLayout = new FRLayout<Node, Edge>(diagram);
        } else {
            final LevelLayout levelLayout = new LevelLayout(diagram);
            levelLayout.adjust = adjust;

            diagramLayout = levelLayout;
        }
        return diagramLayout;
    }

    private String config(final String name, final String defaultValue) {
        final String cst = layout.substring(name.length());
        String len = defaultValue;
        if (!cst.isEmpty()) {
            len = cst;
        }
        return len;
    }

    private JFrame createWindow(final VisualizationViewer<Node, Edge> viewer, final String name) {
        viewer.setBackground(Color.WHITE);

        final DefaultModalGraphMouse<Node, Edge> gm = new DefaultModalGraphMouse<Node, Edge>();
        gm.setMode(DefaultModalGraphMouse.Mode.PICKING);
        viewer.setGraphMouse(gm);

        final JFrame frame = new JFrame(name + " viewer");
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.setLayout(new GridLayout());
        frame.getContentPane().add(viewer);
        frame.pack();

        return frame;
    }

    private void saveView(final Dimension currentSize, final Dimension desiredSize, final String name, final VisualizationViewer<Node, Edge> viewer) {
        BufferedImage bi = new BufferedImage(currentSize.width, currentSize.height, BufferedImage.TYPE_INT_ARGB);

        final Graphics2D g = bi.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);

        final boolean db = viewer.isDoubleBuffered();
        viewer.setDoubleBuffered(false);
        viewer.paint(g);
        viewer.setDoubleBuffered(db);
        if (!currentSize.equals(desiredSize)) {
            final double xFactor = desiredSize.width * 1. / currentSize.width;
            final double yFactor = desiredSize.height * 1. / currentSize.height;
            final double factor = Math.min(xFactor, yFactor);
            info("optimal size is (" + currentSize.width + ", " + currentSize.height + ")");
            info("scaling with a factor of " + factor);

            final AffineTransform tx = new AffineTransform();
            tx.scale(factor, factor);
            final AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_BILINEAR);
            BufferedImage biNew = new BufferedImage((int) (bi.getWidth() * factor), (int) (bi.getHeight() * factor), bi.getType());
            bi = op.filter(bi, biNew);
        }
        g.dispose();

        OutputStream os = null;
        try {
            final File file = new File(output, (outputFileName != null ? outputFileName : name) + "." + format);
            os = new FileOutputStream(file);
            if (!ImageIO.write(bi, format, os)) {
                throw new IllegalStateException("can't save picture " + name + "." + format);
            }
            info("Saved " + file.getAbsolutePath());
        } catch (final IOException e) {
            throw new IllegalStateException("can't save the diagram", e);
        } finally {
            if (os != null) {
                try {
                    os.flush();
                    os.close();
                } catch (final IOException e) {
                    // no-op
                }
            }
        }
    }


    private File validInput() {
        final File file = new File(path);
        if (!file.exists()) {
            final String msg = "Can't find '" + path + "'";
            if (failIfMissing) {
                throw new IllegalStateException(msg);
            }
            warn(msg);
        }
        return file;
    }

    private String slurp(final File file) {
        final String content;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            content = IOUtil.toString(fis);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            IOUtil.close(fis);
        }
        return content;
    }

    private static class Diagram extends DirectedSparseGraph<Node, Edge> {
        private final String name;

        private Diagram(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static class Node {
        public static enum Type {
            STEP, SINK
        }

        private final String text;
        private final Type type;
        private boolean root = false;

        private Node(final String text, final Type type) {
            this.text = text;
            this.type = type;
        }

        public void root() {
            root = true;
        }
    }

    private static class Edge {
        private final String text;

        private Edge(final String text) {
            this.text = text;
        }
    }

    private static class GraphViewer extends VisualizationViewer<Node, Edge> {
        private final boolean rotateEdges;

        public GraphViewer(final Layout<Node, Edge> nodeEdgeLayout, final boolean rotateEdges) {
            super(nodeEdgeLayout);
            this.rotateEdges = rotateEdges;
            init();
        }

        private void init() {
            setOpaque(true);
            setBackground(new Color(255, 255, 255, 0));

            final RenderContext<Node, Edge> context = getRenderContext();

            context.setVertexFillPaintTransformer(new VertexFillPaintTransformer());
            context.setVertexShapeTransformer(new VertexShapeTransformer(getFontMetrics(getFont())));
            context.setVertexLabelTransformer(new VertexLabelTransformer());
            getRenderer().getVertexLabelRenderer().setPosition(Renderer.VertexLabel.Position.CNTR);

            context.setEdgeLabelTransformer(new EdgeLabelTransformer());
            context.setEdgeShapeTransformer(new EdgeShape.Line<Node, Edge>());
            context.setEdgeLabelClosenessTransformer(new EdgeLabelClosenessTransformer());
            context.getEdgeLabelRenderer().setRotateEdgeLabels(rotateEdges);
            getRenderer().setEdgeLabelRenderer(new EdgeLabelRenderer());
        }
    }

    private static class VertexShapeTransformer implements Transformer<Node, Shape> {
        private static final int X_MARGIN = 4;
        private static final int Y_MARGIN = 2;

        private FontMetrics metrics;

        public VertexShapeTransformer(final FontMetrics f) {
            metrics = f;
        }

        @Override
        public Shape transform(final Node i) {
            final int w = metrics.stringWidth(i.text) + X_MARGIN;
            final int h = metrics.getHeight() + Y_MARGIN;

            // centering
            final AffineTransform transform = AffineTransform.getTranslateInstance(-w / 2.0, -h / 2.0);
            switch (i.type) {
                case SINK:
                    return transform.createTransformedShape(new Ellipse2D.Double(0, 0, w, h));
                default:
                    return transform.createTransformedShape(new Rectangle(0, 0, w, h));
            }
        }
    }

    private static class VertexFillPaintTransformer implements Transformer<Node, Paint> {
        @Override
        public Paint transform(final Node node) {
            if (node.root) {
                return Color.GREEN;
            }

            switch (node.type) {
                case SINK:
                    return Color.RED;
                default:
                    return Color.WHITE;
            }
        }
    }

    private static class EdgeLabelTransformer implements Transformer<Edge, String> {
        @Override
        public String transform(Edge i) {
            return i.text;
        }
    }

    private static class VertexLabelTransformer extends ToStringLabeller<Node> {
        public String transform(final Node node) {
            return node.text;
        }
    }

    private static class EdgeLabelClosenessTransformer implements Transformer<Context<Graph<Node, Edge>, Edge>, Number> {
        @Override
        public Number transform(final Context<Graph<Node, Edge>, Edge> context) {
            return 0.5;
        }
    }

    private static class EdgeLabelRenderer extends BasicEdgeLabelRenderer<Node, Edge> {
        public void labelEdge(final RenderContext<Node, Edge> rc, final Layout<Node, Edge> layout, final Edge e, final String label) {
            if (label == null || label.length() == 0) {
                return;
            }

            final Graph<Node, Edge> graph = layout.getGraph();
            // don't draw edge if either incident vertex is not drawn
            final Pair<Node> endpoints = graph.getEndpoints(e);
            final Node v1 = endpoints.getFirst();
            final Node v2 = endpoints.getSecond();
            if (!rc.getEdgeIncludePredicate().evaluate(Context.<Graph<Node, Edge>, Edge>getInstance(graph, e))) {
                return;
            }

            if (!rc.getVertexIncludePredicate().evaluate(Context.<Graph<Node, Edge>, Node>getInstance(graph, v1)) ||
                !rc.getVertexIncludePredicate().evaluate(Context.<Graph<Node, Edge>, Node>getInstance(graph, v2))) {
                return;
            }

            final Point2D p1 = rc.getMultiLayerTransformer().transform(Layer.LAYOUT, layout.transform(v1));
            final Point2D p2 = rc.getMultiLayerTransformer().transform(Layer.LAYOUT, layout.transform(v2));

            final GraphicsDecorator g = rc.getGraphicsContext();
            final Component component = prepareRenderer(rc, rc.getEdgeLabelRenderer(), label, rc.getPickedEdgeState().isPicked(e), e);
            final Dimension d = component.getPreferredSize();

            final AffineTransform old = g.getTransform();
            final AffineTransform xform = new AffineTransform(old);
            final FontMetrics fm = g.getFontMetrics();
            int w = fm.stringWidth(e.text);
            double p = Math.max(0, p1.getX() + p2.getX() - w);
            xform.translate(Math.min(layout.getSize().width - w, p / 2), (p1.getY() + p2.getY() - fm.getHeight()) / 2);
            g.setTransform(xform);
            g.draw(component, rc.getRendererPane(), 0, 0, d.width, d.height, true);

            g.setTransform(old);
        }
    }

    private static class LevelLayout extends AbstractLayout<Node, Edge> {
        private static final int X_MARGIN = 4;

        private Transformer<Node, Shape> vertexShapeTransformer = null;
        private boolean adjust;

        public LevelLayout(final Diagram nodeEdgeGraph) {
            super(nodeEdgeGraph);
        }

        @Override
        public void initialize() {
            final Map<Node, Integer> level = levels();
            final List<List<Node>> nodes = sortNodeByLevel(level);
            final int ySpace = maxHeight(nodes);
            final int nLevels = nodes.size();
            final int yLevel = Math.max(0, getSize().height - nLevels * ySpace) / Math.max(1, nLevels - 1);

            int y = ySpace / 2;
            int maxWidth = getSize().width;
            for (final List<Node> currentNodes : nodes) {
                if (currentNodes.size() == 1) { // only 1 => centering manually
                    setLocation(currentNodes.iterator().next(), new Point(getSize().width / 2, y));
                } else {
                    int x = 0;
                    final int xLevel = Math.max(0, getSize().width - width(currentNodes) - X_MARGIN) / (currentNodes.size() - 1);
                    Collections.sort(currentNodes, new NodeComparator((Diagram) graph, locations));

                    for (Node node : currentNodes) {
                        Rectangle b = getBound(node, vertexShapeTransformer);
                        int step = b.getBounds().width / 2;
                        x += step;
                        setLocation(node, new Point(x, y));
                        x += xLevel + step;
                    }

                    maxWidth = Math.max(maxWidth, x - xLevel);
                }
                y += yLevel + ySpace;
            }

            if (adjust) {
                adjust = false;
                setSize(new Dimension(maxWidth, y + ySpace));
                initialize();
                adjust = true;
            }
        }

        @Override
        public void reset() {
            initialize();
        }

        private int width(List<Node> nodes) {
            int sum = 0;
            for (Node node : nodes) {
                sum += getBound(node, vertexShapeTransformer).width;
            }
            return sum;
        }

        private int maxHeight(final List<List<Node>> nodes) {
            int max = 0;
            for (final List<Node> list : nodes) {
                for (final Node n : list) {
                    max = Math.max(max, getBound(n, vertexShapeTransformer).height);
                }
            }
            return max;
        }

        private Rectangle getBound(final Node n, final Transformer<Node, Shape> vst) {
            if (vst == null) {
                return new Rectangle(0, 0);
            }
            return vst.transform(n).getBounds();
        }

        private List<List<Node>> sortNodeByLevel(final Map<Node, Integer> level) {
            final int levels = max(level);

            final List<List<Node>> sorted = new ArrayList<List<Node>>();
            for (int i = 0; i < levels; i++) {
                sorted.add(new ArrayList<Node>());
            }

            for (final Map.Entry<Node, Integer> entry : level.entrySet()) {
                sorted.get(entry.getValue()).add(entry.getKey());
            }
            return sorted;
        }

        private int max(final Map<Node, Integer> level) {
            int i = 0;
            for (Map.Entry<Node, Integer> l : level.entrySet()) {
                if (l.getValue() >= i) {
                    i = l.getValue() + 1;
                }
            }
            return i;
        }

        private Map<Node, Integer> levels() {
            final Map<Node, Integer> out = new HashMap<Node, Integer>();
            for (final Node node : graph.getVertices()) { // init
                out.put(node, 0);
            }

            final Map<Node, Collection<Node>> successors = new HashMap<Node, Collection<Node>>();
            final Map<Node, Collection<Node>> predecessors = new HashMap<Node, Collection<Node>>();
            for (final Node node : graph.getVertices()) {
                successors.put(node, graph.getSuccessors(node));
                predecessors.put(node, graph.getPredecessors(node));
            }

            boolean done;
            do {
                done = true;
                for (final Node node : graph.getVertices()) {
                    int nodeLevel = out.get(node);
                    for (final Node successor : successors.get(node)) {
                        if (out.get(successor) <= nodeLevel
                            && successor != node
                            && !predecessors.get(node).contains(successor)) {
                            done = false;
                            out.put(successor, nodeLevel + 1);
                        }
                    }
                }
            } while (!done);

            final int min = Collections.min(out.values());
            for (final Map.Entry<Node, Integer> entry : out.entrySet()) {
                out.put(entry.getKey(), entry.getValue() - min);
            }

            return out;
        }
    }

    private static class NodeComparator implements Comparator<Node> { // sort by predecessor location
        private final Diagram graph;
        private final Map<Node, Point2D> locations;

        public NodeComparator(final Diagram diagram, final Map<Node, Point2D> points) {
            graph = diagram;
            locations = points;
        }

        @Override
        public int compare(Node o1, Node o2) {
            final Collection<Node> p1 = graph.getPredecessors(o1);
            final Collection<Node> p2 = graph.getPredecessors(o2);

            // mean value is used but almost always there is only one predecessor
            final int m1 = mean(p1);
            final int m2 = mean(p2);
            return m1 - m2;
        }

        private int mean(final Collection<Node> p) {
            if (p.size() == 0) {
                return 0;
            }
            int mean = 0;
            for (final Node n : p) {
                mean += locations.get(n).getX();
            }
            return mean / p.size();
        }
    }
}
