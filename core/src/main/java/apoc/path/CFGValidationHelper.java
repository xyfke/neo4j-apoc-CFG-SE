package apoc.path;
import org.checkerframework.checker.units.qual.N;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    public enum NodeLabel implements Label {
        cVariable
    }

    public static enum DataflowType {
        PREFIX, SUFFIX, INTRA;
    }

    // helper function: return start and end CFG nodes along with the connections
    // return: a hashset of CFG nodes
    public static HashMap<List<Node>, Relationship> getConnectionNodes(Relationship r, CandidatePath candidatePath,
                                                         boolean isFirst, boolean isReverse) {

        //ArrayList<Node> cfgNodes = new ArrayList<>();
        HashMap<List<Node>, Relationship> cfgNodes = new HashMap<>();   // HashSet<[srcNode, dstNode]> (need dstNode to update CFG)
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
                Relationship nextCFGBlockEdge = null;

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
                                nextCFGBlockEdge = nextCFGRel;
                            } else {
                                addNode = (nextCFGRel.hasProperty("cfgReturn") &&
                                        nextCFGRel.getProperty("cfgReturn").equals("1"));
                                nextCFGBlockEdge = nextCFGRel;
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
                    cfgNodes.put(List.of(srcCFG.getEndNode(), dstCFG.getEndNode()), nextCFGBlockEdge);
                }
            }
        }

        return cfgNodes;


    }

    // helper function: return start and end CFG nodes along with the connections for gm parWrite
    // return: a hashset of CFG nodes
    public static HashMap<List<Node>, Relationship> getParWriteConnectionNodes(Node pwNode,
                                                                               CandidatePath candidatePath,
                                                                                boolean start) {

        HashMap<List<Node>, Relationship> cfgNodes = new HashMap<>();
        Iterable<Relationship> targetCFGs = (start) ?
                pwNode.getRelationships(Direction.OUTGOING, RelTypes.pwDestination) :
                pwNode.getRelationships(Direction.OUTGOING, RelTypes.pwSource);

        for (Relationship targetCFG : targetCFGs) {
            cfgNodes.put(List.of(targetCFG.getEndNode(), targetCFG.getEndNode()), null);
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

    public static void addCFGToCandidatePath(CandidatePath candidatePath,
                                             HashMap<List<Node>, Relationship> startCFGs,
                                             boolean isReverse) {

        HashSet<Node> acceptedCFGs = new HashSet<>();

        for (List<Node> startCFG : startCFGs.keySet()) {
            if (isReverse) {
                acceptedCFGs.add(startCFG.get(1));
            } else {
                acceptedCFGs.add(startCFG.get(0));
            }

        }
        candidatePath.updateCFG(acceptedCFGs);

    }


}
