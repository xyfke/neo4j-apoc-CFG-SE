package apoc.cfgPath;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BasicCandidatePath {

    public int pathIndex;   // index in terms of RelExtension
    public ArrayList<Relationship> path;    // list of all the relationships in the path
    public HashSet<Node> validCFGs;     // records the last validated end CFG nodes
    public int pathSize;    // length of path
    public ArrayList<Relationship> retRel;      // list of retWrites in a path (used for allShortestPath)

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
    public BasicCandidatePath(BasicCandidatePath oldPath, Relationship curEdge, int pathIndex, boolean backward) {
        this.path = new ArrayList<>(oldPath.getPath());
        this.path.add(curEdge);
        this.validCFGs = new HashSet<>(oldPath.getValidCFGs());
        this.pathIndex = pathIndex;
        this.pathSize = oldPath.getPathSize() + 1;
        this.retRel = new ArrayList<>(oldPath.getRetRel());
        if (curEdge.isType(retWrite)) {
            int indexPos = backward ? 0 : this.retRel.size();
            this.retRel.add(indexPos, curEdge);
        }
    }

    // get attributes
    public int getPathSize() {return this.pathSize;}
    public int getPathIndex() {return this.pathIndex;}
    public ArrayList<Relationship> getPath() {return this.path;}
    public HashSet<Node> getValidCFGs() {return this.validCFGs;}
    public ArrayList<Relationship> getRetRel() {return this.retRel;}

    // CFG setter and compare
    public void setValidCFGs(HashSet<Node> validCFGs) {this.validCFGs = validCFGs;}

    // convert list of path to Neo4J path
    public Path buildPath() {
        PathImpl.Builder builder = new PathImpl.Builder(this.path.get(0).getStartNode());

        for (Relationship rel : this.path) {
            builder = builder.push(rel);
        }

        return builder.build();

    }

    // convert list of path to Neo4J path
    public Path reversebuildPath() {
        PathImpl.Builder builder = new PathImpl.Builder(this.path.get(this.path.size()-1).getStartNode());

        for (int i = this.path.size()-1; i > -1; i--) {
            builder = builder.push(this.path.get(i));
        }

        return builder.build();

    }

    // get last edge in the path
    public Relationship getLastEdge() {
        return this.path.get(this.pathSize-1);
    }

    // get last edge in the path
    public Relationship getSecondLastEdge() {
        return this.path.get(this.pathSize-2);
    }

    // compare the list of retWrites with otherPath
    public boolean compareRetNodes(BasicCandidatePath otherPath) {
        if ((!otherPath.getRetRel().isEmpty()) && (!this.retRel.isEmpty())) {
            return this.retRel.get(0).getStartNode().equals(otherPath.getRetRel().get(0).getStartNode());
        } else {
            return false;
        }
    }

    // create all possible retWrite combinations with the list of retWrites of this path
    // e.g. [retWrite1, retWrite2, retWrite3]
    //         [[retWrite1], [retWrite1, retWrite2], [retWrite1, retWrite2, retWrite3]]
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
