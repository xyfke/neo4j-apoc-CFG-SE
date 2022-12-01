package apoc.path;

import org.apache.commons.math3.geometry.spherical.twod.Edge;
import org.checkerframework.checker.units.qual.C;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;

public class CandidatePath {

    public ArrayList<Relationship> partialResult;
    public HashSet<Node> validCFGs;
    public int pathSize;
    //public ArrayList<Stack<Relationship>> callStacks;
    public Node startNode;
    public Node endNode;
    public ArrayList<Relationship> retRel;

    public CandidatePath() {
        this.partialResult = new ArrayList();
        this.validCFGs = new HashSet<Node>();
        this.pathSize = 0;
        //this.callStacks = new ArrayList<>();
        this.retRel = new ArrayList<>();
        this.endNode = null;
    }

    public CandidatePath(Node endNode) {
        this.partialResult = new ArrayList();
        this.validCFGs = new HashSet<Node>();
        this.pathSize = 0;
        //this.callStacks = new ArrayList<>();
        this.endNode = endNode;
        this.retRel = new ArrayList<>();
    }

    // constructor for a single edge
    public CandidatePath(Relationship startEdge) {
        this.partialResult = new ArrayList(List.of(startEdge));
        this.validCFGs = new HashSet<Node>();
        this.pathSize = 1;
        //this.callStacks = new ArrayList<>();
        this.endNode = startEdge.getEndNode();
        this.retRel = new ArrayList<>();

        if (startEdge.getStartNode().hasLabel(CFGValidationHelper.NodeLabel.cReturn)) {
            this.retRel.add(startEdge);
        }
    }

    // constructor to create new path from old path plus a single edge
    public CandidatePath(CandidatePath oldPath, Relationship newEdge) {
        this.partialResult = new ArrayList(oldPath.partialResult);
        this.partialResult.add(newEdge);
        this.validCFGs = oldPath.validCFGs;
        this.pathSize = oldPath.getPathSize() + 1;
        this.endNode = newEdge.getEndNode();
        this.retRel = new ArrayList<>(oldPath.retRel);

        // get all previous callStacks
        /**this.callStacks = new ArrayList<>();
        for (Stack<Relationship> callStack : oldPath.callStacks) {
            Stack<Relationship> tempStack = new Stack<>();
            tempStack.addAll(callStack);
            this.callStacks.add(tempStack);
        }**/

        if (newEdge.getStartNode().hasLabel(CFGValidationHelper.NodeLabel.cReturn)) {
            this.retRel.add(newEdge);
        }
    }

    // constructor to create new path from old path plus a single edge
    public CandidatePath(CandidatePath oldPath) {
        this.partialResult = new ArrayList(oldPath.partialResult);
        this.validCFGs = oldPath.validCFGs;
        this.pathSize = oldPath.getPathSize();
        this.endNode = oldPath.endNode;

        // get all previous callStacks
        /**this.callStacks = new ArrayList<>();
         for (Stack<Relationship> callStack : oldPath.callStacks) {
         Stack<Relationship> tempStack = new Stack<>();
         tempStack.addAll(callStack);
         this.callStacks.add(tempStack);
         }**/

        this.retRel = new ArrayList<>(oldPath.retRel);
    }

    public boolean compareRetNodes(CandidatePath path2) {

        if ((!path2.retRel.isEmpty()) && (!this.retRel.isEmpty())) {
            return this.retRel.get(0).getStartNode().equals(path2.retRel.get(0).getStartNode());
        } else {
            return false;
        }

    }

    // return path information
    // get the last relationship in the path
    public Relationship getLastRel() {
        if (this.pathSize >= 1) {
            return this.partialResult.get(this.pathSize-1);
        } else {
            return null;
        }
    }

    // get the second last relationship in the path
    public Relationship getSecondLastRel() {
        if (this.pathSize >= 2) {
            return this.partialResult.get(this.pathSize-2);
        } else {
            return null;
        }
    }

    // get the end node of last relationship
    public Node getEndNode() {
        return this.endNode;
        /**if (this.pathSize >= 1) {
            return this.partialResult.get(this.pathSize-1).getEndNode();
        } else {
            return null;
        }**/
    }

    // get the start node of the last relationship
    public Node getStartNode() {
        if (this.pathSize >= 1) {
            return this.partialResult.get(this.pathSize-1).getStartNode();
        } else {
            return null;
        }
    }

    // get length of path
    public int getPathSize() {
        return this.pathSize;
    }

    // CFG functions:
    // return valid CFGs
    public void updateCFG(HashSet validCFGs) {
        this.validCFGs = validCFGs;
    }

    // whether or not there are related CFG Blocks
    public boolean isCFGEmpty() {
        return this.validCFGs.isEmpty();
    }

    public boolean hasCFG(Node cfgNode) {
        if (this.isCFGEmpty()) {
            return true;
        } else {
            return this.validCFGs.contains(cfgNode);
        }
    }

    // return path build from partialResult variable
    public Path buildPath() {
        PathImpl.Builder builder = new PathImpl.Builder(this.partialResult.get(0).getStartNode());

        for (Relationship rel : this.partialResult) {
            builder = builder.push(rel);
        }

        return builder.build();

    }

    // return path build backward from partialResult variable
    public Path backwardBuildPath() {
        PathImpl.Builder builder = new PathImpl.Builder(this.partialResult.get(this.pathSize-1).getStartNode());

        for (int i = this.pathSize-1; i >= 0; i--) {
            builder = builder.push(this.partialResult.get(i));
        }

        return builder.build();

    }



}
