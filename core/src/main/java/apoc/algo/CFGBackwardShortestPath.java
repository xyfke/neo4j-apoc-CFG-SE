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

public class CFGBackwardShortestPath {

    private final PathExpander expander;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    public CFGBackwardShortestPath(Transaction tx) {
        this.tx = tx;
        expander = buildPathExpander("<nextCFGBlock");
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
        }

        // get EndNodes
        for (List<Node> cfgEndNode : cfgEndNodes.keySet()) {
            endNodes.add(cfgEndNode.get(0));
            Relationship edge = cfgStartNodes.get(cfgEndNode);
            if (edge != null) { cfgRetInv.add(edge); }
        }

        if ((startNodes.isEmpty()) || (endNodes.isEmpty())) {
            return new ArrayList<>();
        }

        //Transaction transaction = context.transaction();
        CFGBackwardEvaluator evaluator = new CFGBackwardEvaluator(candidatePath, startNodes, cfgRetInv);
        TraversalDescription td = tx.traversalDescription();
        td = td.breadthFirst().uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .evaluator(evaluator).expand(this.expander);

        Traverser traverser = td.traverse(endNodes);
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

    private class CFGBackwardEvaluator implements Evaluator {

        private CandidatePath candidatePath;
        private HashSet<Node> startNodes;
        public ArrayList<Stack<Relationship>> curCallStacks;
        public HashSet<Node> filterOut;
        public HashSet<Relationship> cfgInvRets;

        public CFGBackwardEvaluator(CandidatePath candidatePath, HashSet<Node> startNodes,
                                    HashSet<Relationship> cfgInvRets) {

            this.candidatePath = candidatePath;
            this.startNodes = startNodes;
            this.curCallStacks = candidatePath.callStacks;
            this.filterOut = new HashSet<>();
            this.cfgInvRets = cfgInvRets;

            for (Relationship cfgInvRet : cfgInvRets) {
                if (updateBackwardCallStack(cfgInvRet)) {
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
                if (startNodes.contains(path.endNode())) {
                    return Evaluation.of(true, false);
                } else {
                    return Evaluation.of(false, true);
                }
            }

            Relationship lastEdge = path.lastRelationship();
            if (this.cfgInvRets.contains(lastEdge)) {
                return Evaluation.of(false, false);
            }

            if (updateBackwardCallStack(lastEdge)) {
                if (startNodes.contains(path.endNode())) {
                    return Evaluation.of(true, false);
                } else {
                    return Evaluation.of(false, true);
                }
            } else {
                return Evaluation.of(false, false);
            }
        }

        private boolean updateBackwardCallStack(Relationship edge) {

            boolean continuePath = false;
            boolean isInvoke = false;
            boolean isReturn = false;

            if (edge.hasProperty("cfgInvoke") && (edge.getProperty("cfgInvoke").equals("1"))) {
                isInvoke = true;
            } else if (edge.hasProperty("cfgReturn") && (edge.getProperty("cfgReturn").equals("1"))) {
                isReturn = true;
            } else {
                return true;
            }

            ArrayList<Stack<Relationship>> addCallStack = new ArrayList<>();
            ArrayList<Stack<Relationship>> removeStack = new ArrayList<>();

            for (Stack<Relationship> callStack : this.curCallStacks) {
                Relationship endRel = callStack.get(callStack.size()-1);
                if (isReturn) {
                    if (compareFunction(endRel.getEndNode(), edge.getStartNode())) {
                        Stack<Relationship> newCallStack = new Stack<>();
                        newCallStack.addAll(callStack);
                        newCallStack.add(edge);
                        addCallStack.add(newCallStack);
                        removeStack.add(callStack);
                        continuePath = true;
                    }
                } else if (isInvoke) {
                    if (endRel.getEndNode().equals(edge.getStartNode())) {
                        callStack.pop();
                        continuePath = true;
                    }
                }
            }


            if ((!continuePath) && (isReturn)) {
                Stack<Relationship> newCallStack = new Stack<>();
                newCallStack.add(edge);
                this.curCallStacks.add(newCallStack);
                continuePath = true;
            }

            // get rid of empty call stacks
            this.curCallStacks.removeIf(Stack<Relationship>::isEmpty);
            this.curCallStacks.addAll(addCallStack);
            this.curCallStacks.removeAll(removeStack);

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
