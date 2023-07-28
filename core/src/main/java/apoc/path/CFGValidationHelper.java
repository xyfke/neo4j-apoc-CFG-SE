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
        varWrite, varWriteSource, varWriteDestination,
        parWrite, parWriteSource, parWriteDestination,
        retWrite, retWriteSource, retWriteDestination,
        varInfFunc, varInfFuncSource, varInfFuncDestination,
        varInfluence, varInfluenceSource, varInfluenceDestination,
        call, callSource, callDestination,
        write, writeSource, writeDestination,
        nextCFGBlock,
        pubVar, pubVarSource, pubVarDestination,
        pubTarget, pubTargetSource, pubTargetDestination,
        compCall, compReturn, dataflowOTF, dataflowNCFG;
    }

    public enum NodeLabel implements Label {
        cVariable, cReturn
    }

    public static enum DataflowType {
        PREFIX, SUFFIX, INTRA, ALL;
    }

    // helper function: return start and end CFG nodes along with the connections
    // return: a hashset of CFG nodes
    public static HashSet<List<Node>> getConnectionNodesAll(Relationship edge,
                                                            HashMap<String,
                                                                    CFGPath.CFGSetting> cfgConfig) {

        RelationshipType edgeType = edge.getType();
        String edgeTypeStr = edgeType.name();
        RelationshipType sourceType = RelationshipType.withName(edgeTypeStr + "Source");
        RelationshipType destinationType = RelationshipType.withName(edgeTypeStr + "Destination");
        RelationshipType nextCFGType = RelationshipType.withName("nextCFGBlock");

        // get node label (assume: every node has only one label)
        String startNodeLabel = edge.getStartNode().getLabels().iterator().next().name();
        String endNodeLabel = edge.getEndNode().getLabels().iterator().next().name();

        // get settings input by user
        String configKey = startNodeLabel + edgeTypeStr + endNodeLabel;
        CFGPath.CFGSetting config = cfgConfig.get(configKey);
        int length = (config != null) ? config.getLength() : 0;
        String[] attribute = (config != null) ? config.getAttribute() : null;

        // get the CFG nodes in iterable
        Iterable<Relationship> srcEdges = edge.getStartNode().getRelationships(Direction.OUTGOING, sourceType);
        Iterable<Relationship> dstEdges = edge.getEndNode().getRelationships(Direction.OUTGOING, destinationType);

        // create srcEdges Hashset
        HashSet<List<Node>> relatedNodes = new HashSet<>();
        for (Relationship srcEdge : srcEdges) {
            Node endSrcNode = srcEdge.getEndNode();
            relatedNodes.add(List.of(endSrcNode, endSrcNode));
        }

        // handle length + attribute
        int i = 0;
        while ((attribute != null) && (i < attribute.length)) {
            String attributeName = attribute[i];
            HashSet<List<Node>> tempSets = new HashSet<>();

            for (List<Node> relatedNode : relatedNodes) {
                Iterable<Relationship> tempEdges = relatedNode.get(1).getRelationships(Direction.OUTGOING,
                        nextCFGType);
                for (Relationship tempEdge : tempEdges) {
                    if ((tempEdge.hasProperty(attributeName))) {
                        tempSets.add(List.of(relatedNode.get(0), tempEdge.getEndNode()));
                    }
                }
            }
            relatedNodes = tempSets;
            i++;
        }

        // check for shortest path
        if (length < 0) {
            PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                    new BasicEvaluationContext(tx, db),
                    buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
            );

            HashSet<List<Node>> tempSets = new HashSet<>();
            for (List<Node> relatedNode : relatedNodes) {
                for (Relationship dstEdge : dstEdges) {
                    Path path = algo.findSinglePath(relatedNode.get(1), dstEdge.getEndNode());
                    if (path != null) {
                        tempSets.add(List.of(relatedNode.get(0), dstEdge.getEndNode()));
                    }
                }
            }
            relatedNodes = tempSets;
        } else {
            HashSet<List<Node>> tempSets = new HashSet<>();
            for (List<Node> relatedNode : relatedNodes) {
                for (Relationship dstEdge : dstEdges) {
                    if (dstEdge.getEndNode().equals(relatedNode.get(1))) {
                        tempSets.add(List.of(relatedNode.get(0), dstEdge.getEndNode()));
                    }
                }
            }
            relatedNodes = tempSets;
        }



        return relatedNodes;

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
                    RelTypes.varWriteSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.varWriteDestination);
        } else if (r.isType(RelTypes.parWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.parWriteSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.parWriteDestination);
        } else if (r.isType(RelTypes.retWrite)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.retWriteSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.retWriteDestination);
        } else if (r.isType(RelTypes.varInfFunc)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.varInfFuncSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.varInfFuncDestination);
        } else if (r.isType(RelTypes.varInfluence)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.varInfluenceSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.varInfluenceDestination);
        } else if (r.isType(RelTypes.call)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.callSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.callDestination);
        } else if (r.isType(RelTypes.write)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.writeSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.writeDestination);
        } else if (r.isType(RelTypes.pubVar)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pubVarSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pubVarDestination);
        } else if (r.isType(RelTypes.pubTarget)) {
            srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pubTargetSource);
            dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pubTargetDestination);
        }

        for (Relationship srcCFG : srcCFGs) {
            for (Relationship dstCFG : dstCFGs) {

                boolean addNode = false;
                Relationship nextCFGBlockEdge = null;

                if (r.isType(RelTypes.varWrite) || r.isType(RelTypes.write) || r.isType(RelTypes.pubVar)
                    || r.isType(RelTypes.pubTarget)) {
                    addNode = srcCFG.getEndNode().equals(dstCFG.getEndNode());
                } else if (r.isType(RelTypes.parWrite) || r.isType(RelTypes.retWrite)
                        || r.isType(RelTypes.call)) {

                    Iterable<Relationship> nextCFGRels = srcCFG.getEndNode().getRelationships(Direction.OUTGOING,
                            RelTypes.nextCFGBlock);

                    for (Relationship nextCFGRel : nextCFGRels) {
                        if (dstCFG.getEndNode().equals(nextCFGRel.getEndNode())) {
                            if (r.isType(RelTypes.parWrite) || (r.isType(RelTypes.call))) {
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
    public static HashMap<List<Node>, Relationship> getStartEndNodes(Node node, String type, boolean start) {

        HashMap<List<Node>, Relationship> cfgNodes = new HashMap<>();
        Iterable<Relationship> targetCFGs = null;

        if (type.equals("varWriteOut") || type.equals("varWriteIn")) {
            targetCFGs = (start) ?
                    node.getRelationships(Direction.OUTGOING, RelTypes.varWriteDestination) :
                    node.getRelationships(Direction.OUTGOING, RelTypes.varWriteSource);
        } else if (type.equals("parWriteOut") || type.equals("parWriteIn")) {
            targetCFGs = (start) ?
                    node.getRelationships(Direction.OUTGOING, RelTypes.parWriteDestination) :
                    node.getRelationships(Direction.OUTGOING, RelTypes.parWriteSource);
        } else if (type.equals("retWriteOut") || type.equals("retWriteIn")) {
            targetCFGs = (start) ?
                    node.getRelationships(Direction.OUTGOING, RelTypes.retWriteDestination) :
                    node.getRelationships(Direction.OUTGOING, RelTypes.retWriteSource);
        } else {
            return cfgNodes;
        }

        for (Relationship targetCFG : targetCFGs) {
            cfgNodes.put(List.of(targetCFG.getEndNode(), targetCFG.getEndNode()), null);
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
                pwNode.getRelationships(Direction.OUTGOING, RelTypes.parWriteDestination) :
                pwNode.getRelationships(Direction.OUTGOING, RelTypes.parWriteSource);

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
                acceptedCFGs.add(startCFG.get(0));
            } else {
                acceptedCFGs.add(startCFG.get(1));
            }

        }
        candidatePath.updateCFG(acceptedCFGs);

    }


}
