package apoc.path;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;

import static org.neo4j.graphdb.traversal.Evaluation.*;

public class CFGEvaluator implements Evaluator {

    private DataflowType dataflowType;
    private Relationship additionalRel;
    private boolean cfgCheck;
    private PathExplorer pathExp = new PathExplorer();
    private int pathLength = -1;
    private boolean getAllShortest;
    private boolean foundPath = false;

    private Evaluator endNodeEvaluator;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    // PREFIX constructor
    public CFGEvaluator(Relationship startRel, Node endNode, boolean cfgCheck) {
        this.dataflowType = DataflowType.PREFIX;
        this.additionalRel = startRel;
        this.cfgCheck = cfgCheck;
        this.endNodeEvaluator = Evaluators.includeWhereEndNodeIs(endNode);
    }

    // MIDDLE constructor
    public CFGEvaluator(Node startNode, Node endNode, boolean cfgCheck) {
        this.dataflowType = DataflowType.MIDDLE;
        this.cfgCheck = cfgCheck;
        this.endNodeEvaluator = Evaluators.includeWhereEndNodeIs(endNode);
    }

    // SUFFIX constructor
    public CFGEvaluator(Node startNode, Relationship endRel, boolean cfgCheck) {
        this.dataflowType = DataflowType.SUFFIX;
        this.additionalRel = endRel;
        this.cfgCheck = cfgCheck;
        this.endNodeEvaluator = Evaluators.includeWhereEndNodeIs(endRel.getStartNode());
    }

    public CFGEvaluator(Node endNode, boolean cfgCheck) {
        this.dataflowType = DataflowType.PREFIX;
        this.endNodeEvaluator = Evaluators.includeWhereEndNodeIs(endNode);
        this.cfgCheck = cfgCheck;
    }


    @Override
    public Evaluation evaluate(Path path) {

        Relationship curRel;
        Relationship prevRel;
        boolean found = endNodeEvaluator.evaluate(path).includes();

        // no CFG checking is required
        if (!this.cfgCheck) {
            return Evaluation.of(includePath(found), !this.foundPath);
        }

        // PREFIX, path.length() == 1
        if (path.length() == 0) {
            return Evaluation.of(includePath(found), !this.foundPath);
        } else if (path.length() == 1) {
            if (this.dataflowType == DataflowType.PREFIX) {
                prevRel = this.additionalRel;
                curRel = path.lastRelationship();
            } else {
                return Evaluation.of(includePath(found), !this.foundPath);
            }
        } else {
            ArrayList<Relationship> relInPath = new ArrayList<>();
            path.relationships().forEach(relInPath::add);
            prevRel = relInPath.get(relInPath.size()-2);
            curRel = relInPath.get(relInPath.size()-1);
        }

        ArrayList<Node> cfgBS1 = getCFGNodes(prevRel, true);
        ArrayList<Node> cfgBS2 = getCFGNodes(curRel, false);
        boolean hasPath = hasCFGPath(cfgBS1, cfgBS2);


        if (hasPath) {

            return Evaluation.of(includePath(found), !this.foundPath);

            /**if (this.dataflowType == DataflowType.SUFFIX) {
                if (include) {
                    cfgBS1 = getCFGNodes(curRel, true);
                    cfgBS2 = getCFGNodes(this.additionalRel, false);
                    if (hasCFGPath(cfgBS1, cfgBS2)) {
                        return Evaluation.of(true, false);
                    } else {
                        return Evaluation.of(false, false);
                    }
                } else {
                    return Evaluation.of(false, true);
                }
            } else {
                return Evaluation.of(found, !found);
            }**/

        } else {
            // if not valid CFG, then do not continue or include
            return Evaluation.of(false, false);
        }


    }

    public boolean includePath(boolean isEnd) {
        if (isEnd) {
            if (!this.foundPath) {
                this.foundPath = true;
                return true;
            }
        }
        return false;
    }


    public boolean hasCFGPath(ArrayList<Node> cfgBS1, ArrayList<Node> cfgBS2) {

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                buildPathExpander("nextCFGBlock>"), Integer.MAX_VALUE
        );

        for (Node cfgB1 : cfgBS1) {
            for (Node cfgB2 : cfgBS2) {
                Path cfgPath = algo.findSinglePath(cfgB1, cfgB2);
                if (cfgPath != null) {
                    return true;
                }
            }
        }

        return false;

    }

    // get requested node from a specified iterable of relationships
    public ArrayList<Node> getNodes(Iterable<Relationship> cfgEdges, boolean getEnd) {
        ArrayList<Node> cfgNodes = new ArrayList<>();
        for (Relationship cfgEdge : cfgEdges) {
            if (getEnd) {
                cfgNodes.add(cfgEdge.getEndNode());
            } else {
                cfgNodes.add(cfgEdge.getStartNode());
            }
        }
        return cfgNodes;
    }

    // match source nodes with destination nodes
    public ArrayList<Relationship> getCFGEdge(ArrayList<Node> srcNodes, ArrayList<Node> dstNodes,
                                              boolean isCfgInvoke) {

        ArrayList<Relationship> temporaryCFGEdge = new ArrayList<>();

        for (Node srcNode : srcNodes) {
            Iterable<Relationship> temporaryEdges = srcNode.getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.nextCFGBlock);
            for (Relationship temporaryEdge : temporaryEdges) {
                boolean hasProp;
                if (isCfgInvoke) { hasProp = temporaryEdge.hasProperty("cfgInvoke"); }
                else { hasProp = temporaryEdge.hasProperty("cfgReturn"); }

                if ((isCfgInvoke) && (hasProp) && (temporaryEdge.getProperty("cfgInvoke").equals("1")) &&
                        dstNodes.contains(temporaryEdge.getEndNode())) {
                    temporaryCFGEdge.add(temporaryEdge);
                } else if ((!isCfgInvoke) && (hasProp) && (temporaryEdge.getProperty("cfgReturn").equals("1")) &&
                        dstNodes.contains(temporaryEdge.getEndNode())) {
                    temporaryCFGEdge.add(temporaryEdge);
                }
            }

        }

        return temporaryCFGEdge;

    }

    // get the relevant CFG nodes
    public ArrayList<Node>  getCFGNodes(Relationship edge, boolean isFirst) {

        ArrayList<Node> cfgBlocksS1;
        ArrayList<Node> cfgBlocksS2;

        if (edge.isType(CFGEvaluator.RelType.varWrite)) {
            cfgBlocksS1 = getNodes(edge.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.vwSource), true);
            cfgBlocksS2 = getNodes(edge.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.vwDestination), true);
            cfgBlocksS1.retainAll(cfgBlocksS2);
            return cfgBlocksS1;
        } else if (edge.isType(CFGEvaluator.RelType.parWrite)) {
            cfgBlocksS1 = getNodes(edge.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.pwSource), true);
            cfgBlocksS2 = getNodes(edge.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.pwDestination), true);
            ArrayList<Relationship> cfgInvokeEdges = getCFGEdge(cfgBlocksS1, cfgBlocksS2, true);
            ArrayList<Node> temporaryNodes;
            if (isFirst) {
                temporaryNodes = getNodes(cfgInvokeEdges, true);
                cfgBlocksS2.retainAll(temporaryNodes);
                return cfgBlocksS2;
            } else {
                temporaryNodes = getNodes(cfgInvokeEdges, false);
                cfgBlocksS1.retainAll(temporaryNodes);
                return cfgBlocksS1;
            }

        } else if (edge.isType(CFGEvaluator.RelType.retWrite)) {
            cfgBlocksS1 = getNodes(edge.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.rwSource), true);
            cfgBlocksS2 = getNodes(edge.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.rwDestination), true);

            ArrayList<Relationship> cfgReturnEdges = getCFGEdge(cfgBlocksS1, cfgBlocksS2, false);
            ArrayList<Node> temporaryNodes;
            if (isFirst) {
                temporaryNodes = getNodes(cfgReturnEdges, true);
                cfgBlocksS2.retainAll(temporaryNodes);
                return cfgBlocksS2;
            } else {
                temporaryNodes = getNodes(cfgReturnEdges, false);
                cfgBlocksS1.retainAll(temporaryNodes);
                return cfgBlocksS1;
            }

        } else if (edge.isType(CFGEvaluator.RelType.varInfFunc)) {
            cfgBlocksS1 = getNodes(edge.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.vifSource), true);
            cfgBlocksS2 = getNodes(edge.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGEvaluator.RelType.vifDestination), true);

            PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                    new BasicEvaluationContext(tx, db),
                    buildPathExpander("nextCFGBlock>"), Integer.MAX_VALUE
            );

            ArrayList<Node> returnNodes = new ArrayList<>();

            for (Node cfgB1 : cfgBlocksS1) {
                for (Node cfgB2 : cfgBlocksS2) {
                    Path vifCFG = algo.findSinglePath(cfgB1, cfgB2);
                    if (vifCFG != null) {
                        if (isFirst) {
                            returnNodes.add(cfgB1);
                        } else {
                            returnNodes.add(cfgB2);
                        }
                    }
                }
            }

            return returnNodes;

        }

        return null;

    }

    private PathExpander<Double> buildPathExpander(String relationshipsAndDirections) {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections
                .parse(relationshipsAndDirections)) {
            if (pair.first() == null) {
                if (pair.other() == null) {
                    builder = PathExpanderBuilder.allTypesAndDirections();
                } else {
                    builder = PathExpanderBuilder.allTypes(pair.other());
                }
            } else {
                if (pair.other() == null) {
                    builder = builder.add(pair.first());
                } else {
                    builder = builder.add(pair.first(), pair.other());
                }
            }
        }
        return builder.build();
    }


    // enum indicating where dataflow path belongs
    public enum DataflowType {
        PREFIX, MIDDLE, SUFFIX
    }

    // enum for constructing path
    private enum RelType implements RelationshipType {
        varWrite, vwSource, vwDestination,
        parWrite, pwSource, pwDestination,
        retWrite, rwSource, rwDestination,
        varInfFunc, vifSource, vifDestination,
        varInfluence, viSource, viDestination,
        nextCFGBlock
    }
}
