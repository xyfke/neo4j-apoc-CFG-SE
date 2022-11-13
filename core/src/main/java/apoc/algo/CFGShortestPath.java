package apoc.algo;

import apoc.path.RelationshipTypeAndDirections;
import org.neo4j.graphdb.*;

import static org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_GLOBAL;

import org.neo4j.graphalgo.EvaluationContext;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;

import apoc.path.CandidatePath;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CFGShortestPath {

    //private final EvaluationContext context;
    private final PathExpander expander;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    public CFGShortestPath(Transaction tx) {
        this.tx = tx;
        expander = buildPathExpander("nextCFGBlock>");
    }

    public List<Path> findPath(HashMap<List<Node>, Relationship> cfgStartNodes,
                               HashMap<List<Node>, Relationship> cfgEndNodes,
                               CandidatePath candidatePath) {

        HashSet<Node> startNodes = new HashSet<>();
        HashSet<Node> endNodes = new HashSet<>();
        HashSet<Relationship> cfgRetInv = new HashSet<>();

        // get StartNodes
        for (List<Node> cfgStartNode : cfgStartNodes.keySet()) {
            startNodes.add(cfgStartNode.get(1));
            Relationship edge = cfgStartNodes.get(cfgStartNode);
            if (edge != null) { cfgRetInv.add(edge); }
        }

        // get EndNodes
        for (List<Node> cfgEndNode : cfgEndNodes.keySet()) {
            endNodes.add(cfgEndNode.get(0));
        }

        if ((startNodes.isEmpty()) || (endNodes.isEmpty())) {
            return new ArrayList<>();
        }

        //Transaction transaction = context.transaction();
        CFGEvaluator evaluator = new CFGEvaluator(candidatePath, endNodes, cfgRetInv);
        TraversalDescription td = tx.traversalDescription();
        td = td.breadthFirst().uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .evaluator(evaluator).expand(this.expander);

        Traverser traverser = td.traverse(startNodes);
        Stream<Path> results = Iterables.stream(traverser);
        List<Path> path = results.collect(Collectors.toList());

        return path;
    }

    private static PathExpander<Double> buildPathExpander(String relationshipsAndDirections) {
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

    private class CFGEvaluator implements Evaluator {

        private CandidatePath candidatePath;
        private HashSet<Node> endNodes;
        public HashSet<Stack<Relationship>> curCallStacks;
        public HashSet<Node> filterOut;
        public HashSet<Relationship> cfgInvRets;

        public CFGEvaluator(CandidatePath candidatePath, HashSet<Node> endNodes,
                           HashSet<Relationship> cfgInvRets) {
            this.candidatePath = candidatePath;
            this.endNodes = endNodes;
            this.curCallStacks = candidatePath.callStacks;
            this.filterOut = new HashSet<>();
            this.cfgInvRets = cfgInvRets;

            for (Relationship cfgInvRet : cfgInvRets) {
                if (!updateCallStack(cfgInvRet)) {
                    this.filterOut.add(cfgInvRet.getEndNode());
                }
            }
        }

        @Override
        public Evaluation evaluate(Path path) {

            if (path.length() == 0) {
                // if path starts with filter node, then omit completely
                if (this.filterOut.contains(path.endNode())) {
                    return Evaluation.of(false, false);
                }

                // otherwise just check for endNodes
                if (endNodes.contains(path.endNode())) {
                    return Evaluation.of(true, false);
                } else {
                    return Evaluation.of(false, true);
                }
            }

            Relationship lastEdge = path.lastRelationship();
            if (this.cfgInvRets.contains(lastEdge)) {
                return Evaluation.of(false, false);
            }
            if (updateCallStack(lastEdge)) {
                if (endNodes.contains(path.endNode())) {
                    return Evaluation.of(true, false);
                } else {
                    return Evaluation.of(false, true);
                }
            } else {
                return Evaluation.of(false, false);
            }

        }

        private boolean updateCallStack(Relationship edge) {

            boolean continuePath = false;
            boolean isInvoke = false;
            boolean isReturn = false;
            boolean addedToStack = false;

            if (edge.hasProperty("cfgInvoke") && (edge.getProperty("cfgInvoke").equals("1"))) {
                isInvoke = true;
            } else if (edge.hasProperty("cfgReturn") && (edge.getProperty("cfgReturn").equals("1"))) {
                isReturn = true;
            } else {
                return true;
            }

            for (Stack<Relationship> callStack : this.curCallStacks) {
                Relationship endRel = callStack.get(callStack.size()-1);
                if (isInvoke) {
                    if (compareFunction(endRel.getEndNode(), edge.getStartNode())) {
                        Stack<Relationship> newCallStack = new Stack<>();
                        newCallStack.addAll(callStack);
                        newCallStack.add(edge);
                        this.curCallStacks.add(newCallStack);
                        continuePath = true;
                    }
                } else if (isReturn) {
                    if (endRel.getStartNode().equals(edge.getEndNode())) {
                        callStack.pop();
                        continuePath = true;
                    }
                }
            }


            if ((!continuePath) && (isInvoke)) {
                Stack<Relationship> newCallStack = new Stack<>();
                newCallStack.add(edge);
                this.curCallStacks.add(newCallStack);
                continuePath = true;
            }

            // get rid of empty call stacks
            this.curCallStacks.removeIf(Stack<Relationship>::isEmpty);

            return continuePath;

        }

        private boolean compareFunction(Node node1, Node node2) {
            String idNode1 = (String) node1.getProperty("id");
            String[] functionNameNode1 = idNode1.split(";;:");
            String idNode2 = (String) node2.getProperty("id");
            String[] functionNameNode2 = idNode2.split(";;:");
            return functionNameNode1[0].equals(functionNameNode2[0]);
        }
    }



}
