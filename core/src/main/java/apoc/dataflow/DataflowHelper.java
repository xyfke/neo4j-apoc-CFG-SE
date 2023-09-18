package apoc.dataflow;

import apoc.path.CFGValidationHelper;
import apoc.path.CandidatePath;
import apoc.path.RelationshipTypeAndDirections;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;

import java.util.HashMap;
import java.util.List;

public class DataflowHelper {

    @Context
    public static GraphDatabaseService db;

    @Context
    public static Transaction tx;

    public enum RelTypes implements RelationshipType
    {
        varWrite, vwSource, vwDestination,
        parWrite, pwSource, pwDestination,
        retWrite, rwSource, rwDestination,
        varInfFunc, vifSource, vifDestination,
        varInfluence, viSource, viDestination,
        nextCFGBlock, pubVar, pubTarget;
    }

    public enum DataflowType {
        PREFIX, SUFFIX, MIDDLE;
    }

    // helper function: finds outgoing dataflow edge connected to current node
    // return: a list of these outgoing dataflow edge
    public static Iterable<Relationship> getNextRels(Node current) {
        return current.getRelationships(Direction.OUTGOING, CFGValidationHelper.RelTypes.varWrite,
                CFGValidationHelper.RelTypes.parWrite, CFGValidationHelper.RelTypes.retWrite);
    }

    // helper function: return start and end CFG nodes along with the connections
    // return: a hashset of CFG nodes
    public static HashMap<List<Node>, Relationship> getConnectionNodes(Relationship r) {

        if (r == null) {
            return null;
        }


        HashMap<List<Node>, Relationship> cfgNodes = new HashMap<>();   // HashSet<[srcNode, dstNode]> (need dstNode to update CFG)
        Iterable<Relationship> srcCFGs = null;
        Iterable<Relationship> dstCFGs = null;

        // get all potential source and destination nodes
        if (r.isType(CFGValidationHelper.RelTypes.varWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.varWriteSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.varWriteDestination);
        } else if (r.isType(CFGValidationHelper.RelTypes.parWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.parWriteSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.parWriteDestination);
        } else if (r.isType(CFGValidationHelper.RelTypes.retWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.retWriteSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.retWriteDestination);
        } else if (r.isType(CFGValidationHelper.RelTypes.varInfFunc)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.varInfFuncSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.varInfFuncDestination);
        } else if (r.isType(CFGValidationHelper.RelTypes.varInfluence)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.varInfluenceSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    CFGValidationHelper.RelTypes.varInfluenceDestination);
        }

        for (Relationship srcCFG : srcCFGs) {
            for (Relationship dstCFG : dstCFGs) {

                boolean addNode = false;
                Relationship nextCFGBlockEdge = null;

                if (r.isType(CFGValidationHelper.RelTypes.varWrite)) {
                    addNode = srcCFG.getEndNode().equals(dstCFG.getEndNode());
                } else if (r.isType(CFGValidationHelper.RelTypes.parWrite) || r.isType(CFGValidationHelper.RelTypes.retWrite)) {

                    Iterable<Relationship> nextCFGRels = srcCFG.getEndNode().getRelationships(Direction.OUTGOING,
                            CFGValidationHelper.RelTypes.nextCFGBlock);

                    for (Relationship nextCFGRel : nextCFGRels) {
                        if (dstCFG.getEndNode().equals(nextCFGRel.getEndNode())) {
                            if (r.isType(CFGValidationHelper.RelTypes.parWrite)) {
                                addNode = (nextCFGRel.hasProperty("cfgInvoke") &&
                                        nextCFGRel.getProperty("cfgInvoke").equals("1"));
                                nextCFGBlockEdge = nextCFGRel;
                            } else {
                                addNode = (nextCFGRel.hasProperty("cfgReturn") &&
                                        nextCFGRel.getProperty("cfgReturn").equals("1"));
                                nextCFGBlockEdge = nextCFGRel;
                            }

                            if (addNode) {break;}

                        }
                    }

                }  else if (r.isType(CFGValidationHelper.RelTypes.varInfFunc) || r.isType(CFGValidationHelper.RelTypes.varInfluence)) {
                    PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                            new BasicEvaluationContext(tx, db),
                            buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
                    );
                    Path vifCFG = algo.findSinglePath(srcCFG.getEndNode(), dstCFG.getEndNode());
                    addNode = (vifCFG != null);
                }

                if (addNode) {
                    cfgNodes.put(List.of(srcCFG.getEndNode(), dstCFG.getEndNode()), nextCFGBlockEdge);
                }
            }
        }

        return cfgNodes;


    }

    public static PathExpander<Double> buildPathExpander(String relationshipsAndDirections) {
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

}
