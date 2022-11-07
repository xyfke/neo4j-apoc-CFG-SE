package apoc.path;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class CFGValidationHelper {

    @Context
    public static GraphDatabaseService db;

    @Context
    public static Transaction tx;


    // define used relationship types
    public enum RelTypes implements RelationshipType
    {
        varWrite, vwSource, vwDestination,
        parWrite, pwSource, pwDestination,
        retWrite, rwSource, rwDestination,
        varInfFunc, vifSource, vifDestination,
        varInfluence, viSource, viDestination,
        nextCFGBlock, pubVar, pubTarget;
    }

    public static enum DataflowType {
        PREFIX, SUFFIX, INTRA;
    }

    // helper function: return start and end CFG nodes along with the connections
    // return: a hashset of CFG nodes
    public static HashSet<List<Node>> getConnectionNodes(Relationship r, CandidatePath candidatePath,
                                                         boolean isFirst, boolean isReverse) {

        //ArrayList<Node> cfgNodes = new ArrayList<>();
        HashSet<List<Node>> cfgNodes = new HashSet<>();   // HashSet<[srcNode, dstNode]> (need dstNode to update CFG)
        Iterable<Relationship> srcCFGs = null;
        Iterable<Relationship> dstCFGs = null;

        if (r.isType(RelTypes.varWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vwSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vwDestination);
        } else if (r.isType(RelTypes.parWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pwSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pwDestination);
        } else if (r.isType(RelTypes.retWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.rwSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.rwDestination);
        } else if (r.isType(RelTypes.varInfFunc)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vifSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vifDestination);
        } else if (r.isType(RelTypes.varInfluence)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.viSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.viDestination);
        }

        for (Relationship srcCFG : srcCFGs) {
            for (Relationship dstCFG : dstCFGs) {

                boolean addNode = false;

                if (r.isType(RelTypes.varWrite)) {
                    addNode = srcCFG.getEndNode().equals(dstCFG.getEndNode());
                } else if (r.isType(RelTypes.parWrite) || r.isType(RelTypes.retWrite)) {

                    Iterable<Relationship> nextCFGRels = srcCFG.getEndNode().getRelationships(Direction.OUTGOING,
                            RelTypes.nextCFGBlock);

                    for (Relationship nextCFGRel : nextCFGRels) {
                        if (dstCFG.getEndNode().equals(nextCFGRel.getEndNode())) {
                            if (r.isType(RelTypes.parWrite)) {
                                addNode = (nextCFGRel.hasProperty("cfgInvoke") &&
                                        nextCFGRel.getProperty("cfgInvoke").equals("1"));
                            } else {
                                addNode = (nextCFGRel.hasProperty("cfgReturn") &&
                                        nextCFGRel.getProperty("cfgReturn").equals("1"));
                            }

                            if (addNode) {break;}

                        }
                    }

                }  else if (r.isType(RelTypes.varInfFunc) || r.isType(RelTypes.varInfluence)) {
                    PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                            new BasicEvaluationContext(tx, db),
                            buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
                    );
                    Path vifCFG = algo.findSinglePath(srcCFG.getEndNode(), dstCFG.getEndNode());
                    addNode = (vifCFG != null);
                }

                if (addNode) {
                    if (isFirst) {
                        if ((isReverse) || (candidatePath.hasCFG(dstCFG.getEndNode()))) {
                            cfgNodes.add(List.of(dstCFG.getEndNode(), srcCFG.getEndNode()));
                        }
                    } else {
                        if ((!isReverse) || (candidatePath.hasCFG(srcCFG.getEndNode()))) {
                            cfgNodes.add(List.of(srcCFG.getEndNode(), dstCFG.getEndNode()));
                        }
                    }
                }

            }
        }

        return cfgNodes;


    }

    // helper function: return start and end CFG nodes along with the connections for gm parWrite
    // return: a hashset of CFG nodes
    public static HashSet<List<Node>> getParWriteConnectionNodes(Relationship r, CandidatePath candidatePath,
                                                          boolean first) {

        HashSet<List<Node>> cfgNodes = new HashSet<>();

        if (r.isType(RelTypes.parWrite)) {
            Iterable<Relationship> targetCFGs;
            if (first) {
                targetCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                        RelTypes.pwDestination);
            } else {
                targetCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                        RelTypes.pwSource);
            }

            for (Relationship targetCFG : targetCFGs) {
                if (first) {
                    if (candidatePath.hasCFG(targetCFG.getEndNode())) {
                        cfgNodes.add(List.of(targetCFG.getEndNode(), targetCFG.getEndNode()));
                    }
                } else {
                    cfgNodes.add(List.of(targetCFG.getEndNode(), targetCFG.getEndNode()));
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

    // helper function: finds outgoing dataflow edge connected to current node
    // return: a list of these outgoing dataflow edge
    public static Iterable<Relationship> getNextRels(Node current, boolean isPrefix) {
        if (isPrefix) {
            return current.getRelationships(Direction.OUTGOING, RelTypes.varWrite);
        } else {
            return current.getRelationships(Direction.OUTGOING, RelTypes.varWrite, RelTypes.parWrite, RelTypes.retWrite);
        }
    }

    // helper function: finds incoming dataflow edge connected to current node
    // return: a list of these incoming dataflow edge
    public static Iterable<Relationship> getPrevRels(Node current, boolean isPrefix) {
        if (isPrefix) {
            return current.getRelationships(Direction.INCOMING, RelTypes.varWrite);
        } else {
            return current.getRelationships(Direction.INCOMING, RelTypes.varWrite, RelTypes.parWrite, RelTypes.retWrite);
        }
    }

}
