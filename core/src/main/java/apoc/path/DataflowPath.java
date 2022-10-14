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

    @UserFunction
    @Description("apoc.path.dataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a dataflow path consisting of dataflow relationships from one variable to another")
    public Path dataflowPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                           @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                           @Name("cfgCheck") boolean cfgCheck) {

        Node start;
        Node end;
        int category;

        // define needed variables
        HashSet<Relationship> visitedEdge = new HashSet<Relationship>();
        Queue<ArrayList<Relationship>> queuePath = new LinkedList<>();
        ArrayList<Relationship> curRels = new ArrayList<Relationship>();
        Relationship vifEdge = null;
        PathImpl.Builder builder;


        if ((startNode != null) && (endNode != null)) {         // dataflow in middle component
            start = startNode;
            end = endNode;
            category = 1;
            builder = new PathImpl.Builder(startNode);
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            start = startNode;
            end = endEdge.getStartNode();
            category = 2;
            builder = new PathImpl.Builder(startNode);
            vifEdge = endEdge;
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            start = startEdge.getEndNode();
            end = endNode;
            category = 3;
            curRels.add(startEdge);
            queuePath.add(curRels);
            builder = new PathImpl.Builder(startEdge.getStartNode());
        } else {                                                // not valid
            return null;
        }

        // If no further search is required (start equals to end)
        if ((start.equals(end))) {
            if (vifEdge != null) {
                curRels.add(vifEdge);
            }
            return buildPath(builder, curRels);
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != 3) {
            dataflowRels = getNextRels(start, false);

            // add the relationships connected to start node
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdge.contains(dataflowRel)) {
                    ArrayList<Relationship> relList = new ArrayList<Relationship>();
                    relList.add(dataflowRel);
                    queuePath.add(relList);
                }
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;

        while (!queuePath.isEmpty()) {
            // get first ArrayList<Relationship> item off of queuePath
            curRels = queuePath.poll(); // get the array of dataflow relationship

            // get the last relationship in the ArrayList path
            Relationship curRel = curRels.get(curRels.size()-1);
            Node curNode = curRel.getStartNode();
            Node nextNode = curRel.getEndNode();

            // check size of existing path
            if (curRels.size() == 1) {
                // if end node matches and length of path is 1 then return path without verifying CFG
                if (nextNode.equals(end)) {

                    // If suffix, then we need to do an additional CFG check before return
                    if ((vifEdge != null) && (cfgCheck)) {
                        cfgPath = getCFGPath(curRel, vifEdge);
                    } else {
                        cfgPath = new ArrayList<Relationship>();
                    }

                    if ((!cfgCheck) || (cfgPath != null)) {
                        if (vifEdge != null) {curRels.add(vifEdge); }
                        return buildPath(builder, curRels);
                    }

                    continue;

                // otherwise, add the relationship connected to the next node to the queue
                // then continue with the search
                } else {
                    visitedEdge.add(curRel);
                    dataflowRels = getNextRels(nextNode, false);
                    for (Relationship dataflowRel : dataflowRels) {
                        // only add not visited Nodes
                        if (!visitedEdge.contains(dataflowRel)) {
                            ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                            newCurRels.add(dataflowRel);
                            queuePath.add(newCurRels);
                        }
                    }
                    continue;
                }
            }

            // if path contains 2 or more relationships, then we need to verify the cfg path prior to adding
            Relationship prevRel = curRels.get(curRels.size()-2);
            Node prevNode = prevRel.getStartNode();

            if (cfgCheck) {
                cfgPath = getCFGPath(prevRel, curRel);
            } else {
                cfgPath = new ArrayList<Relationship>();
            }

            // if there exists a CFG path, then this is a valid path, and we can continue, otherwise drop path
            if (cfgPath != null) {
                visitedEdge.add(curRel);

                // if the nextNode happens to be equal to end node, then we found the path
                if (nextNode.equals(end)) {
                    // If suffix, then we need to do an additional CFG check before return
                    if ((vifEdge != null) && (cfgCheck)) {
                        cfgPath = getCFGPath(curRel, vifEdge);
                    } else {
                        cfgPath = new ArrayList<Relationship>();
                    }

                    if ((!cfgCheck) || (cfgPath != null)) {
                        if (vifEdge != null) {curRels.add(vifEdge); }
                        return buildPath(builder, curRels);
                    } else {
                        continue;
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

    @UserFunction
    @Description("apoc.path.allDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck) - finds a dataflow path consisting of dataflow relationships from one variable to another")
    public List<Path> allDataflowPaths(@Name("startNode") Node startNode, @Name("endNode") Node endNode,
                                 @Name("startEdge") Relationship startEdge, @Name("endEdge") Relationship endEdge,
                                 @Name("cfgCheck") boolean cfgCheck) {

        Node start;
        Node end;
        int category;

        // define needed variables
        HashSet<Relationship> visitedEdges = new HashSet<Relationship>();
        Queue<ArrayList<Relationship>> queuePath = new LinkedList<>();
        ArrayList<Relationship> curRels = new ArrayList<Relationship>();
        Relationship vifEdge = null;
        PathImpl.Builder builder;

        List<List<Relationship>> returnRels = new ArrayList<List<Relationship>>();


        if ((startNode != null) && (endNode != null)) {         // dataflow in middle component
            start = startNode;
            end = endNode;
            category = 1;
            builder = new PathImpl.Builder(startNode);
        } else if ((startNode != null) && (endEdge != null)) {  // suffix
            start = startNode;
            end = endEdge.getStartNode();
            category = 2;
            builder = new PathImpl.Builder(startNode);
            vifEdge = endEdge;
        } else if ((startEdge != null) && (endNode != null)) {  // prefix
            start = startEdge.getEndNode();
            end = endNode;
            category = 3;
            curRels.add(startEdge);
            queuePath.add(curRels);
            builder = new PathImpl.Builder(startEdge.getStartNode());
        } else {                                                // not valid
            return null;
        }

        // If no further search is required (start equals to end)
        // nothing else will be shorter than this
        if ((start.equals(end))) {
            if (vifEdge != null) {
                curRels.add(vifEdge);
            }
            return List.of(buildPath(builder, curRels));
        }

        Iterable<Relationship> dataflowRels;

        // if it is not prefix, because we already have a starting edge for prefix, no need to look for the first
        if (category != 3) {
            dataflowRels = getNextRels(start, false);

            // add the relationships connected to start node
            for (Relationship dataflowRel : dataflowRels) {
                if (!visitedEdges.contains(dataflowRel)) {
                    ArrayList<Relationship> relList = new ArrayList<Relationship>();
                    relList.add(dataflowRel);
                    queuePath.add(relList);
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
            // get first ArrayList<Relationship> item off of queuePath
            curRels = queuePath.poll(); // get the array of varWrite-parWrite relationship
            int curLen = curRels.size();

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

            // get the last relationship in the ArrayList path
            Relationship curRel = curRels.get(curRels.size()-1);
            Node nextNode = curRel.getEndNode();

            // check size of existing path
            if (curRels.size() == 1) {
                // if end node matches and length of path is 1 then return path without verifying CFG
                if (nextNode.equals(end)) {

                    // If suffix, then we need to do an additional CFG check before return
                    if ((vifEdge != null) && (cfgCheck)) {
                        cfgPath = getCFGPath(curRel, vifEdge);
                    } else {
                        cfgPath = new ArrayList<Relationship>();
                    }

                    if ((!cfgCheck) || (cfgPath != null)) {
                        if (vifEdge != null) {curRels.add(vifEdge); }
                        returnRels.add(curRels);
                        foundPath = true;
                    }

                    continue;

                // otherwise, add the relationship connected to the next node to the queue
                // then continue with the search
                } else {
                    visitedEdge.add(curRel);
                    dataflowRels = getNextRels(nextNode, false);
                    for (Relationship dataflowRel : dataflowRels) {
                        // only add unused relationships
                        if (!visitedEdges.contains(dataflowRel)) {
                            ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                            newCurRels.add(dataflowRel);
                            queuePath.add(newCurRels);
                        }
                    }
                    continue;
                }
            }

            // if path contains 2 or more relationships, then we need to verify the cfg path prior to adding
            Relationship prevRel = curRels.get(curRels.size()-2);

            if (cfgCheck) {
                cfgPath = getCFGPath(prevRel, curRel);
            } else {
                cfgPath = new ArrayList<Relationship>();
            }

            // if there exists a CFG path, then this is a valid path, and we can continue, otherwise drop path
            if (cfgPath != null) {
                // since CFG has been verified, then prevRel is confirmed to be visited and exited
                visitedEdge.add(prevRel);

                // if the nextNode happens to be equal to end node, then we found the path
                if (nextNode.equals(end)) {

                    // If suffix, then we need to do an additional CFG check before return
                    if ((vifEdge != null) && (cfgCheck)) {
                        cfgPath = getCFGPath(curRel, vifEdge);
                    } else {
                        cfgPath = new ArrayList<Relationship>();
                    }

                    if ((!cfgCheck) || (cfgPath != null)) {
                        if (vifEdge != null) {curRels.add(vifEdge); }
                        returnRels.add(curRels);
                        foundPath = true;
                    }

                    continue;
                }

                // otherwise keep looking
                dataflowRels = getNextRels(nextNode, false);
                // only add not visited Nodes
                for (Relationship dataflowRel : dataflowRels) {
                    if (!visitedEdges.contains(dataflowRel)) {
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(dataflowRel);
                        queuePath.add(newCurRels);
                    }
                }
            }

        }

        List<Path> returnPaths = new ArrayList<Path>();
        for (List<Relationship> rels : returnRels) {
            if ((rels != null) && (rels.size() > 0)) {
                returnPaths.add(buildPath(rels.get(0).getStartNode(), (ArrayList<Relationship>) rels));
            }
        }
        return returnPaths;

    }

    // previous implementation that keeps track of visited nodes instead of visited edges
    @UserFunction
    @Description("apoc.path.varParPath2(start, end, isPrefix, cfgCheck) - finds a dataflow path consisting of varWrites/parWrites/retWrites from one variable to another")
    public Path dataflowPath2(@Name("start") Node start, @Name("end") Node end, @Name("isPrefix") boolean isPrefix,
                           @Name("cfgCheck") boolean cfgCheck) {

        // terminates path if not exist
        if ((start == null) || (end == null)) {
            return null;
        }

        if ((start.equals(end))) {
            if (isPrefix) { return null; }
            else { return buildPath(start, null); }
        }

        // define needed variables
        HashSet<Node> visitedNode = new HashSet<Node>();
        Queue<ArrayList<Relationship>> queuePath = new LinkedList<>();
        ArrayList<Relationship> curRels = null;

        // add start to visitedNode, and add relationships connected to start to queuePath
        visitedNode.add(start);
        PathImpl.Builder builder = new PathImpl.Builder(start);
        Iterable<Relationship> varWriteRels;

        varWriteRels = getNextRels(start, isPrefix);

        // add the relationships connected to start node
        for (Relationship varWriteRel : varWriteRels) {
            if (!visitedNode.contains(varWriteRel.getEndNode())) {
                ArrayList<Relationship> relList = new ArrayList<Relationship>();
                relList.add(varWriteRel);
                queuePath.add(relList);
            }
        }

        // cfgPath variable
        List<Relationship> cfgPath = null;

        while (!queuePath.isEmpty()) {
            // get first ArrayList<Relationship> item off of queuePath
            curRels = queuePath.poll(); // get the array of varWrite-parWrite relationship

            // get the last relationship in the ArrayList path
            Relationship curRel = curRels.get(curRels.size()-1);
            Node curNode = curRel.getStartNode();
            Node nextNode = curRel.getEndNode();

            // check size of existing path
            if (curRels.size() == 1) {
                // if end node matches and length of path is 1 then return path without verifying CFG
                if (nextNode.equals(end)) {
                    builder = builder.push(curRel);
                    return builder.build();
                    // otherwise, add the relationship connected to the next node to the queue
                    // then continue with the search
                } else {
                    visitedNode.add(nextNode);
                    varWriteRels = getNextRels(nextNode, false);
                    if (varWriteRels == null) {
                        continue;
                    }
                    for (Relationship varWriteRel : varWriteRels) {
                        // only add not visited Nodes
                        if (!visitedNode.contains(varWriteRel.getEndNode())) {
                            ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                            newCurRels.add(varWriteRel);
                            queuePath.add(newCurRels);
                        }
                    }
                    continue;
                }
            }

            // if path contains 2 or more relationships, then we need to verify the cfg path prior to adding
            Relationship prevRel = curRels.get(curRels.size()-2);
            Node prevNode = prevRel.getStartNode();

            if (cfgCheck) {
                cfgPath = getCFGPath(prevRel, curRel);
            } else {
                cfgPath = new ArrayList<Relationship>();
            }

            // if there exists a CFG path, then this is a valid path, and we can continue, otherwise drop path
            if (cfgPath != null) {
                visitedNode.add(nextNode);

                // if the nextNode happens to be equal to end node, then we found the path
                if (nextNode.equals(end)) {
                    break;
                }

                // otherwise keep looking
                varWriteRels = getNextRels(nextNode, false);
                if (varWriteRels == null) {
                    continue;
                }
                // only add not visited Nodes
                for (Relationship varWriteRel : varWriteRels) {
                    if (!visitedNode.contains(varWriteRel.getEndNode())) {
                        ArrayList<Relationship> newCurRels = new ArrayList<Relationship>(curRels);
                        newCurRels.add(varWriteRel);
                        queuePath.add(newCurRels);
                    }
                }
            }

        }

        // only return path, if there exists one
        if ((curRels != null) && (curRels.size() > 0) &&
                (curRels.get(curRels.size()-1).getEndNode().equals(end)) && (cfgPath != null)) {
            return buildPath(start, curRels);
        }
        else {
            return null;
        }

    }

    // helper function: find and verify CFG path
    // returns:
    //      - list of relationships: there is a valid path
    //      - null: invalid path
    @UserFunction
    @Description("apoc.path.getCFGPath(r1, r2)")
    public List<Relationship> getCFGPath(@Name("r1") Relationship r1, @Name("r2") Relationship r2) {

        if ((r1 == null) || (r2 == null)) {
            return null;
        }

        PathFinder<Path> algo = GraphAlgoFactory.shortestPath(
                new BasicEvaluationContext(tx, db),
                buildPathExpander("nextCFGBlock>"), (int) Integer.MAX_VALUE
        );

        // obtain cfg nodes and relationships associated with r1 and r2
        HashMap<Node, ArrayList<Relationship>> startCFGGraph = getConnectionNodes(r1, true);
        HashMap<Node, ArrayList<Relationship>> endCFGGraph = getConnectionNodes(r2, false);

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
    public HashMap<Node, ArrayList<Relationship>> getConnectionNodes(Relationship r, boolean first) {

        //HashMap<Node, ArrayList<Relationship>> srcCFGConns = new HashMap<>();
        HashMap<Node, ArrayList<Relationship>> allCFGConns = new HashMap<>();


        if (r.isType(RelTypes.varWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.vwDestination);

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
        } else if (r.isType(RelTypes.parWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.pwDestination);

            // get common end nodes and store in vr1CFGConn
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                Iterable<Relationship> nextCFGRels = srcCFGNode.getRelationships(Direction.OUTGOING,
                        RelTypes.nextCFGBlock);
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

        } else if (r.isType(RelTypes.retWrite)) {
            Iterable<Relationship> srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                    RelTypes.rwSource);
            Iterable<Relationship> dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                    RelTypes.rwDestination);

            // get common end nodes and store in vr1CFGConn
            for (Relationship srcCFG : srcCFGs) {
                Node srcCFGNode = srcCFG.getEndNode();
                Iterable<Relationship> nextCFGRels = srcCFGNode.getRelationships(Direction.OUTGOING,
                        RelTypes.nextCFGBlock);
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
        } else if ((r.isType(RelTypes.varInfFunc)) || (r.isType(RelTypes.varInfluence))) {

            Iterable<Relationship> srcCFGs;
            Iterable<Relationship> dstCFGs;

            if (r.isType(RelTypes.varInfFunc)) {
                srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                        RelTypes.vifSource);
                dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                        RelTypes.vifDestination);
            } else {
                srcCFGs = r.getStartNode().getRelationships(Direction.OUTGOING,
                        RelTypes.viSource);
                dstCFGs = r.getEndNode().getRelationships(Direction.OUTGOING,
                        RelTypes.viDestination);
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
