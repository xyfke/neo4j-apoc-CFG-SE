package apoc.path;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.*;
import org.neo4j.logging.Log;

import java.util.*;

public class DataflowPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Log log;
    // define used relationship types
    private enum RelTypes implements RelationshipType
    {
        varWrite, vwSource, vwDestination,
        parWrite, pwSource, pwDestination,
        retWrite, rwSource, rwDestination,
        varInfFunc, vifSource, vifDestination,
        varInfluence, viSource, viDestination,
        nextCFGBlock, pubVar, pubTarget;
    }

    private enum DataflowType {
        PREFIX, SUFFIX, INTRA
    }

    @UserFunction
    @Description("apoc.path.dataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a dataflow path consisting of dataflow relationships from one variable to another")
    public Path dataflowPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                           @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                           @Name("cfgCheck") boolean cfgCheck) {

        //Node start;
        Node end;
        DataflowType category;  // indicating what type of dataflow path we are working with

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();
        CandidatePath curPath = null;


        if ((startNode != null) && (endNode != null)) {         // dataflow in middle components
            end = endNode;
            category = DataflowType.INTRA;
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            end = endEdge.getStartNode();
            category = DataflowType.SUFFIX;
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            end = endNode;
            category = DataflowType.PREFIX;
            curPath = new CandidatePath(startEdge);
            queuePath.add(curPath);
        } else {                                                // not valid
            return null;
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != DataflowType.PREFIX) {
            dataflowRels = getNextRels(startNode, false);

            // add the relationships connected to start node
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdge.contains(dataflowRel)) {
                    CandidatePath candidatePath = new CandidatePath(dataflowRel);
                    queuePath.add(candidatePath);
                }
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;

        while (!queuePath.isEmpty()) {

            // get the last path
            curPath = queuePath.poll();

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (getCFGPath(curPath))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    if (category == DataflowType.SUFFIX) {
                        CandidatePath vifPath = new CandidatePath(curPath, endEdge);
                        if ((!cfgCheck) || (getCFGPath(vifPath))) {
                            // build path, and return (exit)
                            return vifPath.buildPath();
                        }
                    } else {
                        // build path, and return (exit)
                        return curPath.buildPath();
                    }

                }

                dataflowRels = getNextRels(curPath.getEndNode(), false);
                for (Relationship dataflowRel : dataflowRels) {
                    if (!visitedEdge.contains(dataflowRel)) {
                        CandidatePath newCandidatePath = new CandidatePath(curPath, dataflowRel);
                        queuePath.add(newCandidatePath);
                    }
                }

            }

        }

        return null;

    }

    @UserFunction
    @Description("apoc.path.allDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a dataflow path consisting of dataflow relationships from one variable to another")
    public List<Path> allDataflowPaths(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                                 @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                                 @Name("cfgCheck") boolean cfgCheck) {

        //Node start;
        Node end;
        DataflowType category;  // indicating what type of dataflow path we are working with

        // define needed variables
        HashSet<Relationship> visitedEdges = new HashSet<Relationship>();
        Queue<CandidatePath> queuePath = new LinkedList<>();
        CandidatePath curPath = null;

        List<CandidatePath> returnCandidates = new ArrayList<CandidatePath>();

        if ((startNode != null) && (endNode != null)) {         // dataflow in middle components
            end = endNode;
            category = DataflowType.INTRA;
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            end = endEdge.getStartNode();
            category = DataflowType.SUFFIX;
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            end = endNode;
            category = DataflowType.PREFIX;
            curPath = new CandidatePath(startEdge);
            queuePath.add(curPath);
        } else {                                                // not valid
            return null;
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != DataflowType.PREFIX) {
            dataflowRels = getNextRels(startNode, false);

            // add the relationships connected to start node
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdges.contains(dataflowRel)) {
                    CandidatePath candidatePath = new CandidatePath(dataflowRel);
                    queuePath.add(candidatePath);
                }
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;
        int pathLen = -1;
        boolean foundPath = false;

        // keep track of visited relationships at current length
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();

        while (!queuePath.isEmpty()) {

            curPath = queuePath.poll();
            int curLen = curPath.getPathSize();

            if (foundPath && curLen > pathLen) {
                // if path has been found and current path is longer than found path, can break
                break;
            }

            if (curLen > pathLen) {
                // add all relationships found at previous path length to visitedRels
                visitedEdges.addAll(visitedEdge);
                visitedEdge = new HashSet<Relationship>();
            }
            pathLen = curLen;

            // continue searching only if does not require cfg check or cfg check passes
            if ((!cfgCheck) || (getCFGPath(curPath))) {

                visitedEdge.add(curPath.getLastRel());

                // check if we reach end node
                if (curPath.getEndNode().equals(end)) {

                    if (category == DataflowType.SUFFIX) {
                        CandidatePath vifPath = new CandidatePath(curPath, endEdge);
                        if ((!cfgCheck) || (getCFGPath(vifPath))) {
                            // build path, and return (exit)
                            foundPath = true;
                            returnCandidates.add(vifPath);
                        }
                    } else {
                        // build path, and return (exit)
                        foundPath = true;
                        returnCandidates.add(curPath);
                    }
                }

                dataflowRels = getNextRels(curPath.getEndNode(), false);
                for (Relationship dataflowRel : dataflowRels) {
                    if (!visitedEdges.contains(dataflowRel)) {
                        CandidatePath newCandidatePath = new CandidatePath(curPath, dataflowRel);
                        queuePath.add(newCandidatePath);
                    }
                }

            }

        }

        List<Path> returnPaths = new ArrayList<Path>();
        for (CandidatePath returnCandidate : returnCandidates) {
            if (returnCandidate.getPathSize() > 0) {
                returnPaths.add(returnCandidate.buildPath());
            }
        }

        return returnPaths;

    }

    // helper function: find and verify CFG path
    // returns:
    //      - list of relationships: there is a valid path
    //      - null: invalid path
    public boolean getCFGPath(CandidatePath candidatePath) {

        if (candidatePath.getPathSize() < 2) {
            return true;
        }

        Relationship curRel = candidatePath.getSecondLastRel();
        Relationship nextRel = candidatePath.getLastRel();

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
        );

        // obtain cfg nodes and relationships associated with r1 and r2
        HashSet<List<Node>> startCFGs = getConnectionNodes(curRel, candidatePath, true);
        HashSet<List<Node>> endCFGs = getConnectionNodes(nextRel, candidatePath, false);

        Path cfgPath = null;
        Node n1 = null;
        Node n2 = null;

        if ((startCFGs.isEmpty()) || (endCFGs.isEmpty())) {
            return false;
        }

        HashSet<Node> acceptedCFGEnd = new HashSet<>();

        // if we can find a path from the cfg node associated with r1 to the cfg node associated
        // with r2, then there exists a cfg path
        for (List<Node> listStartCFG : startCFGs) {
            Node startCFGNode = listStartCFG.get(0);
            for (List<Node> listEndCFG : endCFGs) {
                cfgPath = algo.findSinglePath(startCFGNode, listEndCFG.get(0));
                if (cfgPath != null) {
                    acceptedCFGEnd.add(listEndCFG.get(1));
                }
            }
        }

        candidatePath.updateCFG(acceptedCFGEnd);

        return !acceptedCFGEnd.isEmpty();

    }

    // helper function: finds outgoing dataflow edge connected to current node
    // return: a list of these outgoing dataflow edge
    public Iterable<Relationship> getNextRels(Node current, boolean isPrefix) {
        if (isPrefix) {
            return current.getRelationships(Direction.OUTGOING, RelTypes.varWrite);
        } else {
            return current.getRelationships(Direction.OUTGOING, RelTypes.varWrite, RelTypes.parWrite, RelTypes.retWrite);
        }
    }

    // helpher function: merge first and second paths together
    // return: a single path with first and second path merged together
    public Path combine(@Name("first") Path first, @Name("second") Path second) {
        if (first == null) return second;
        if (second == null) return first;

        if (!first.endNode().equals(second.startNode()))
            throw new IllegalArgumentException("Paths don't connect on their end and start-nodes "+first+ " with "+second);

        PathImpl.Builder builder = new PathImpl.Builder(first.startNode());
        for (Relationship rel : first.relationships()) builder = builder.push(rel);
        for (Relationship rel : second.relationships()) builder = builder.push(rel);
        return builder.build();
    }

    // helper function: create a new path with start as the first node, and joined by the edges in listRels
    // return: a single path with all the edges in listRels joint together
    public Path buildPath(Node start, ArrayList<Relationship> listRels) {

        if (start == null) {
            return null;
        }

        PathImpl.Builder builder = new PathImpl.Builder(start);
        if (listRels == null) {
            return builder.build();
        }

        for (Relationship rel : listRels) {
            builder = builder.push(rel);
        }

        return builder.build();

    }

    // helper function: create a new path joined by the edges in listRels
    // return: a single path with all the edges in listRels joint together
    public Path buildPath(PathImpl.Builder builder, ArrayList<Relationship> listRels) {

        if (listRels == null) {
            return builder.build();
        }

        for (Relationship rel : listRels) {
            builder = builder.push(rel);
        }

        return builder.build();

    }


    // helper function: return start and end CFG nodes along with the connections
    // return: a hashmap of CFG node as key and associated list of CFG relationships as value
    public HashSet<List<Node>> getConnectionNodes(Relationship r, CandidatePath candidatePath,
                                                                     boolean first) {

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
                    if (first) {
                        if (candidatePath.hasCFG(dstCFG.getEndNode())) {
                            cfgNodes.add(List.of(dstCFG.getEndNode(), srcCFG.getEndNode()));
                        }
                    } else {
                        cfgNodes.add(List.of(srcCFG.getEndNode(), dstCFG.getEndNode()));
                    }
                }

            }
        }

        return cfgNodes;


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

}
