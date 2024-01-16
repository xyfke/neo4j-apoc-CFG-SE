package apoc.cfgPath;

import apoc.path.CFGPath;
import apoc.path.RelationshipTypeAndDirections;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;

import java.util.*;

public class CFGValidationHelper {

    @Context
    public static GraphDatabaseService db;

    @Context
    public static Transaction tx;

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
                                                                    CFGSetting> cfgConfig) {

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
        CFGSetting config = cfgConfig.get(configKey);
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
