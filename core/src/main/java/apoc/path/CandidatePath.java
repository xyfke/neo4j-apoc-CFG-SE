package apoc.path;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;

public class CandidatePath {

    public ArrayList<Relationship> partialResult;
    public HashSet<Node> validCFGs;
    public int pathSize;

    public CandidatePath(Relationship startEdge) {
        this.partialResult = new ArrayList(List.of(startEdge));
        this.validCFGs = new HashSet<Node>();
        this.pathSize = 1;
    }

    public CandidatePath(CandidatePath oldPath, Relationship newEdge) {
        this.partialResult = new ArrayList(oldPath.partialResult);
        this.partialResult.add(newEdge);
        this.validCFGs = oldPath.validCFGs;
        this.pathSize = oldPath.getPathSize() + 1;
    }

    public void updateCFG(HashSet validCFGs) {
        this.validCFGs = validCFGs;
    }

    public Relationship getLastRel() {
        if (this.pathSize >= 1) {
            return this.partialResult.get(this.pathSize-1);
        } else {
            return null;
        }
    }

    public Relationship getSecondLastRel() {
        if (this.pathSize >= 2) {
            return this.partialResult.get(this.pathSize-2);
        } else {
            return null;
        }
    }

    public Node getEndNode() {
        if (this.pathSize >= 1) {
            return this.partialResult.get(this.pathSize-1).getEndNode();
        } else {
            return null;
        }
    }

    public int getPathSize() {
        return this.pathSize;
    }

    public boolean isCFGEmpty() {
        return this.validCFGs.isEmpty();
    }

    public ArrayList<Relationship> getPartialResult() {
        return this.partialResult;
    }

    public void addToPartialResult(Relationship edge) {
        this.partialResult.add(edge);
        this.pathSize += 1;
    }

    public Path buildPath() {
        PathImpl.Builder builder = new PathImpl.Builder(this.partialResult.get(0).getStartNode());

        for (Relationship rel : this.partialResult) {
            builder = builder.push(rel);
        }

        return builder.build();

    }

    public boolean hasCFG(Node cfgNode) {
        if (this.isCFGEmpty()) {
            return true;
        } else {
            return this.validCFGs.contains(cfgNode);
        }
    }

}
