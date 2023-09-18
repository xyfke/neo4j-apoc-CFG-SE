package apoc.cfgPath;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComponentHelper {

    @UserFunction
    @Description("apoc.cfgPath.isUniqueComponent()")
    public boolean isUniqueComponent(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels) {

        Set<String> set = new HashSet<String>();

        for (Node node: nodes) {
            if (!set.add(node.getProperties("compName").toString())) return false;
        }

        for (Relationship rel : rels) {
            if (!set.add(rel.getProperties("compName").toString())) return false;
        }

        return true;
    }

}
