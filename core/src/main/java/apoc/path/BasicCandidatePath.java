package apoc.path;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BasicCandidatePath {

    public int pathIndex;
    public ArrayList<Relationship> path;
    public HashSet<Node> validCFGs;
    public int pathSize;
    public ArrayList<Relationship> retRel;

    public static RelationshipType retWrite = RelationshipType.withName("retWrite");

    // constructor for single edge
    public BasicCandidatePath(Relationship startEdge, int pathIndex) {
        this.path = new ArrayList<>(List.of(startEdge));
        this.validCFGs = new HashSet<>();
        this.pathIndex = pathIndex;
        this.pathSize = 1;
        this.retRel = (startEdge.isType(retWrite)) ?
                new ArrayList<>(List.of(startEdge)) : new ArrayList<>();
    }

    // constructor for appending to old path
    public BasicCandidatePath(BasicCandidatePath oldPath, Relationship curEdge, int pathIndex) {
        this.path = new ArrayList<>(oldPath.getPath());
        this.path.add(curEdge);
        this.validCFGs = new HashSet<>(oldPath.getValidCFGs());
        this.pathIndex = pathIndex;
        this.pathSize = oldPath.getPathSize() + 1;
        this.retRel = new ArrayList<>(oldPath.getRetRel());
        if (curEdge.isType(retWrite)) { this.retRel.add(curEdge); }
    }

    // get attributes
    public int getPathSize() {return this.pathSize;}
    public int getPathIndex() {return this.pathIndex;}
    public ArrayList<Relationship> getPath() {return this.path;}
    public HashSet<Node> getValidCFGs() {return this.validCFGs;}
    public ArrayList<Relationship> getRetRel() {return this.retRel;}

    // CFG setter and compare
    public void setValidCFGs(HashSet<Node> validCFGs) {this.validCFGs = validCFGs;}
    public boolean isCFGEmpty() {return this.validCFGs.isEmpty(); }
    public boolean hasCFG(Node cfgNode) { return this.validCFGs.contains(cfgNode); }

    // return path build from partialResult variable
    public Path buildPath() {
        PathImpl.Builder builder = new PathImpl.Builder(this.path.get(0).getStartNode());

        for (Relationship rel : this.path) {
            builder = builder.push(rel);
        }

        return builder.build();

    }

    // get path related information
    public Relationship getLastEdge() {
        return this.path.get(this.pathSize-1);
    }

    public Relationship getSecondLastEdge() {
        return (this.pathSize <= 1) ? null : this.path.get(this.pathSize-2);
    }

    // return nodes compare
    public boolean compareRetNodes(BasicCandidatePath otherPath) {
        if ((!otherPath.getRetRel().isEmpty()) && (!this.retRel.isEmpty())) {
            return this.retRel.get(0).getStartNode().equals(otherPath.getRetRel().get(0).getStartNode());
        } else {
            return false;
        }
    }

    // create all possible retWrite combinations
    public ArrayList<ArrayList<Relationship>> getRetComp() {
        ArrayList<ArrayList<Relationship>> visitedComps = new ArrayList<>();
        ArrayList<Relationship> comp = new ArrayList<>();

        for (Relationship retEdge : this.retRel) {
            comp = new ArrayList<>(comp);
            comp.add(retEdge);
            visitedComps.add(comp);
        }

        return visitedComps;
    }




}
