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


public class gmDataflowPath {

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
        PREFIX, INTRA, SUFFIX
    }



    @UserFunction
    @Description("apoc.path.gmDataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a dataflow path consisting of dataflow relationships from one variable to another")
    public Path gmDataflowPath(@Name("startEdge") Relationship startEdge,
                               @Name("endEdge") Relationship endEdge,
                             @Name("cfgCheck") boolean cfgCheck) {

        Node start = startEdge.getEndNode();
        Node end = endEdge.getStartNode();
        DataflowType dType;

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<ArrayList<Relationship>> queuePath = new LinkedList<>();
        ArrayList<Relationship> curRels = new ArrayList<Relationship>();
        PathImpl.Builder builder = new PathImpl.Builder(startEdge.getStartNode());
        curRels.add(startEdge);
        visitedEdge.add(startEdge);

        // cfgPath variable
        List<Relationship> cfgPath = null;


        if ((startEdge.isType(RelTypes.varWrite)) && (endEdge.isType(RelTypes.parWrite))) {
            dType = DataflowType.PREFIX;
        } else if ((startEdge.isType(RelTypes.parWrite)) && (endEdge.isType(RelTypes.parWrite))) {
            dType = DataflowType.INTRA;
        } else if ((startEdge.isType(RelTypes.parWrite)) && (endEdge.isType(RelTypes.varInfFunc))) {
            dType = DataflowType.SUFFIX;
        } else {
            return null;
        }


        // If no further search is required (start equals to end)
        if ((start.equals(end))) {
            if (cfgCheck) {
                if (dType == DataflowType.PREFIX) {
                    cfgPath = getCFGPath(startEdge, endEdge, false, true);
                } else if (dType == DataflowType.INTRA) {
                    cfgPath = getCFGPath(startEdge, endEdge, true, true);
                } else if (dType == DataflowType.SUFFIX) {
                    cfgPath = getCFGPath(startEdge, endEdge, true, false);
                } else {
                    cfgPath = null;
                }

                if (cfgPath != null) {
                    curRels.add(endEdge);
                    return buildPath(builder, curRels);
                }
            } else {
                curRels.add(endEdge);
                return buildPath(builder, curRels);
            }
        }

        Iterable<Relationship> dataflowRels = getNextRels(start, false);

        // add the relationships connected to start node
        for (Relationship dataflowRel : dataflowRels) {
            if (!visitedEdge.contains(dataflowRel)) {
                ArrayList<Relationship> relList = new ArrayList<Relationship>(curRels);
                relList.add(dataflowRel);
                queuePath.add(relList);
            }
        }


        while (!queuePath.isEmpty()) {
            // get first ArrayList<Relationship> item off of queuePath
            curRels = queuePath.poll(); // get the array of dataflow relationship

            // get the last relationship in the ArrayList path
            Relationship curRel = curRels.get(curRels.size()-1);
            Node curNode = curRel.getStartNode();
            Node nextNode = curRel.getEndNode();

            // if path contains 2 or more relationships, then we need to verify the cfg path prior to adding
            Relationship prevRel = curRels.get(curRels.size()-2);
            Node prevNode = prevRel.getStartNode();

            if (cfgCheck) {
                cfgPath = getCFGPath(prevRel, curRel, false, false);
            } else {
                cfgPath = new ArrayList<Relationship>();
            }

            // if there exists a CFG path, then this is a valid path, and we can continue, otherwise drop path
            if (cfgPath != null) {
                visitedEdge.add(curRel);

                // if the nextNode happens to be equal to end node, then we found the path
                if (nextNode.equals(end)) {

                    if (cfgCheck) {

                        cfgPath = (dType == DataflowType.SUFFIX) ?
                                getCFGPath(curRel, endEdge, false, false) :
                                getCFGPath(curRel, endEdge, false, true);

                        if (cfgPath != null) {
                            curRels.add(endEdge);
                            return buildPath(builder, curRels);
                        }

                    } else {
                        curRels.add(endEdge);
                        return buildPath(builder, curRels);
                    }

                }

                // otherwise keep looking
                dataflowRels = getNextRels(nextNode, false);

                // only add not visited edges
                for (Relationship dataflowRel : dataflowRels) {
                    if (!visitedEdge.contains(dataflowRel)) {
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(dataflowRel);
                        queuePath.add(newCurRels);
                    }
                }
            }

        }

        return null;

    }


    // helper function: find and verify CFG path
    // returns:
    //      - list of relationships: there is a valid path
    //      - null: invalid path
    public List<Relationship> getCFGPath(Relationship r1, Relationship r2,
                                         boolean isStartPW, boolean isEndPW) {

        if ((r1 == null) || (r2 == null)) {
            return null;
        }

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
        );

        // obtain cfg nodes and relationships associated with r1 and r2

        HashMap<Node, ArrayList<Relationship>> startCFGGraph = (isStartPW) ?
                getParWriteConnectionNodes(r1, true) : getConnectionNodes(r1, true);
        HashMap<Node, ArrayList<Relationship>> endCFGGraph = (isEndPW) ?
                getParWriteConnectionNodes(r2, false) : getConnectionNodes(r2, false);

        Path cfgPath = null;
        Node n1 = null;
        Node n2 = null;

        if ((startCFGGraph.isEmpty()) || (endCFGGraph.isEmpty())) {
            return null;
        }


        // if we can find a path from the cfg node associated with r1 to the cfg node associated
        // with r2, then there exists a cfg path
        for (Node n1t : startCFGGraph.keySet()) {
            for (Node n2t : endCFGGraph.keySet()) {
                cfgPath = algo.findSinglePath(n1t, n2t);
                if (cfgPath != null) {
                    n1 = n1t; // store the cfg node for r1
                    n2 = n2t; // store the cfg node for r2
                    break;
                }
            }
            if (cfgPath != null) {
                break;
            }
        }

        // only return cfg graph if there exists one
        if ((cfgPath == null) || (n1 == null) || (n2 == null)) {
            return null;
        } else {
            Iterable<Relationship> cfgRels = cfgPath.relationships();
            ArrayList<Relationship> cfgGraph = startCFGGraph.get(n1);
            ArrayList<Relationship> endCFGRels = endCFGGraph.get(n2);


            if ((cfgGraph == null)) {
                return null;
            }

            if ((endCFGRels == null)) {
                return null;
            }

            // add relaitonships associated with n1
            for (Relationship endCFGRel : endCFGRels) {
                cfgGraph.add(endCFGRel);
            }

            // add connecting cfg nodes and relationships connecting n1 to n2
            if (cfgRels != null) {
                for (Relationship cfgRel : cfgRels) {
                    cfgGraph.add(cfgRel);
                }
            }

            return cfgGraph;
        }

    }

    // helper function: finds outgoing dataflow edge connected to current node
    // return: a list of these outgoing dataflow edge
    public Iterable<Relationship> getNextRels(Node current, boolean isPrefix) {
        if (isPrefix) {
            return current.getRelationships(Direction.OUTGOING, gmDataflowPath.RelTypes.varWrite);
        } else {
            return current.getRelationships(Direction.OUTGOING, gmDataflowPath.RelTypes.varWrite, gmDataflowPath.RelTypes.parWrite, gmDataflowPath.RelTypes.retWrite);
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
    public HashMap<Node, ArrayList<Relationship>> getConnectionNodes(Relationship r, boolean first) {

        //HashMap<Node, ArrayList<Relationship>> srcCFGConns = new HashMap<>();
        HashMap<Node, ArrayList<Relationship>> allCFGConns = new HashMap<>();


        if (r.isType(gmDataflowPath.RelTypes.varWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    gmDataflowPath.RelTypes.vwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    gmDataflowPath.RelTypes.vwDestination);

            // get common nodes and store in vr1CFGConns
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                for (Relationship dstCFG : dstCFGs) {
                    Node dstCFGNode = dstCFG.getEndNode();
                    if (srcCFGNode.equals(dstCFGNode)) {
                        ArrayList<Relationship> cfgConnRels = new ArrayList<Relationship>();
                        cfgConnRels.add(srcCFG);
                        cfgConnRels.add(dstCFG);
                        allCFGConns.put(srcCFGNode, cfgConnRels);
                    }
                }
            }
        } else if (r.isType(gmDataflowPath.RelTypes.parWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    gmDataflowPath.RelTypes.pwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    gmDataflowPath.RelTypes.pwDestination);

            // get common end nodes and store in vr1CFGConn
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                Iterable<Relationship> nextCFGRels = srcCFGNode.getRelationships(Direction.OUTGOING,
                        gmDataflowPath.RelTypes.nextCFGBlock);
                HashMap<Node, Relationship> invokeCFGRel = new HashMap<>();

                // filter out the invoke CFG nodes
                for (Relationship nextCFGRel : nextCFGRels) {
                    boolean hasProp=nextCFGRel.hasProperty("cfgInvoke");
                    if ((hasProp) && (nextCFGRel.getProperty("cfgInvoke").equals("1"))) {
                        invokeCFGRel.put(nextCFGRel.getEndNode(), nextCFGRel);
                    }
                }

                // filter out nodes
                for (Relationship dstCFG : dstCFGs) {
                    Node dstCFGNode = dstCFG.getEndNode();
                    Relationship invokeCFGConn = invokeCFGRel.get(dstCFGNode);
                    if (invokeCFGConn != null) {
                        ArrayList<Relationship> cfgConnRels = new ArrayList<Relationship>();
                        cfgConnRels.add(srcCFG);
                        cfgConnRels.add(dstCFG);
                        cfgConnRels.add(invokeCFGConn);

                        // determine node to return as key
                        if (first) {
                            allCFGConns.put(dstCFGNode, cfgConnRels);
                        } else {
                            allCFGConns.put(srcCFGNode, cfgConnRels);
                        }
                    }
                }


            }

        } else if (r.isType(gmDataflowPath.RelTypes.retWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    gmDataflowPath.RelTypes.rwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    gmDataflowPath.RelTypes.rwDestination);

            // get common end nodes and store in vr1CFGConn
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                Iterable<Relationship> nextCFGRels = srcCFGNode.getRelationships(Direction.OUTGOING,
                        gmDataflowPath.RelTypes.nextCFGBlock);
                HashMap<Node, Relationship> invokeCFGRel = new HashMap<>();

                // filter out the invoke CFG nodes
                for (Relationship nextCFGRel : nextCFGRels) {
                    boolean hasProp = nextCFGRel.hasProperty("cfgReturn");
                    if ((hasProp) && (nextCFGRel.getProperty("cfgReturn").equals("1"))) {
                        invokeCFGRel.put(nextCFGRel.getEndNode(), nextCFGRel);
                    }
                }

                // filter out nodes
                for (Relationship dstCFG : dstCFGs) {
                    Node dstCFGNode = dstCFG.getEndNode();
                    Relationship invokeCFGConn = invokeCFGRel.get(dstCFGNode);
                    if (invokeCFGConn != null) {
                        ArrayList<Relationship> cfgConnRels = new ArrayList<Relationship>();
                        cfgConnRels.add(srcCFG);
                        cfgConnRels.add(dstCFG);
                        cfgConnRels.add(invokeCFGConn);

                        // determine node to return as key
                        if (first) {
                            allCFGConns.put(dstCFGNode, cfgConnRels);
                        } else {
                            allCFGConns.put(srcCFGNode, cfgConnRels);
                        }
                    }
                }
            }
        } else if ((r.isType(gmDataflowPath.RelTypes.varInfFunc)) || (r.isType(gmDataflowPath.RelTypes.varInfluence))) {

            Iterable<Relationship> srcCFGs;
            Iterable<Relationship> dstCFGs;

            if (r.isType(gmDataflowPath.RelTypes.varInfFunc)) {
                srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                        gmDataflowPath.RelTypes.vifSource);
                dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                        gmDataflowPath.RelTypes.vifDestination);
            } else {
                srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                        gmDataflowPath.RelTypes.viSource);
                dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                        gmDataflowPath.RelTypes.viDestination);
            }

            PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                    new BasicEvaluationContext(tx, db),
                    buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
            );

            for (Relationship srcCFG : srcCFGs) {
                for (Relationship dstCFG : dstCFGs) {
                    Path vifCFG = algo.findSinglePath(srcCFG.getEndNode(), dstCFG.getEndNode());
                    if (vifCFG != null) {
                        ArrayList<Relationship> cfgConnRels = new ArrayList<Relationship>();
                        cfgConnRels.add(srcCFG);
                        cfgConnRels.add(dstCFG);
                        for (Relationship vifRel : vifCFG.relationships()) {
                            cfgConnRels.add(vifRel);
                        }
                        allCFGConns.put(srcCFG.getEndNode(), cfgConnRels);
                    }
                }
            }




        }

        return allCFGConns;

    }

    // helper function: return start and end CFG nodes along with the connections
    // return: a hashmap of CFG node as key and associated list of CFG relationships as value
    public HashMap<Node, ArrayList<Relationship>> getParWriteConnectionNodes(Relationship r, boolean first) {

        //HashMap<Node, ArrayList<Relationship>> srcCFGConns = new HashMap<>();
        HashMap<Node, ArrayList<Relationship>> allCFGConns = new HashMap<>();

         if (r.isType(gmDataflowPath.RelTypes.parWrite)) {
             Iterable<Relationship> targetCFGs;
             if (first) {
                 targetCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                         RelTypes.pwDestination);
             } else {
                 targetCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                         RelTypes.pwSource);
             }

            for (Relationship targetCFG : targetCFGs) {
                ArrayList<Relationship> relatedCFG = new ArrayList<>();
                relatedCFG.add(targetCFG);
                allCFGConns.put(targetCFG.getEndNode(), relatedCFG);
            }

        }

        return allCFGConns;

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
