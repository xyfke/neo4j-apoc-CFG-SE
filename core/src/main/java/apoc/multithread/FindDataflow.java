package apoc.multithread;

import apoc.result.PathResult;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class FindDataflow {
    public static Set<Long> visitedEdges;

    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("apoc.multithread.findDataflow")
    public List<Path> findDataflow(@Name("startRel") Relationship startRel,
                                   @Name("endRel") Relationship endRel,
                                   @Name("cpus") long cpus) throws IOException {
        BlockingQueue<DataflowEdge> queue = new LinkedBlockingQueue<>();
        BlockingQueue<DataflowEdge> results = new LinkedBlockingQueue<>();
        int threads = (int) cpus;
        ExecutorService service = Executors.newFixedThreadPool(threads);
        visitedEdges = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < threads; ++i) {
            service.execute(new Worker(queue, results));
        }

        try (Transaction tx = db.beginTx()) {
            visitedEdges.add(endRel.getId());
            queue.add(new DataflowEdge(startRel, null));
        } catch (Exception e) {
            e.printStackTrace();
        }

        DataflowEdge result;
        ArrayList<Path> paths = new ArrayList<>();
        do {
            result = null;
            try {
                result = results.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (result == null) {
                break;
            }

            if (result.getCurRelNode().getId() == endRel.getStartNode().getId()) {
                paths.add(constructPath(result, endRel));
            }

        } while (true);

        try {
            service.shutdown();
            service.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        } finally {
            if (!service.isTerminated()) {
                System.err.println("cancel tasks");
            }
            service.shutdownNow();
            System.out.println("shutdown finished");
        }

        return paths;

    }


    public Path constructPath(DataflowEdge dfEdge, Relationship endRel) {
        PathImpl.Builder builder = constructPathRecursive(dfEdge);
        builder = builder.push(endRel);
        return builder.build();
    }

    public PathImpl.Builder constructPathRecursive(DataflowEdge dfEdge) {
        PathImpl.Builder builder;
        Relationship curRel = dfEdge.getCurRel();
        if (dfEdge.getPrevEdge() == null) {
            builder = new PathImpl.Builder(curRel.getStartNode());
        } else {
            builder = constructPathRecursive(dfEdge.getPrevEdge());
        }
        builder = builder.push(curRel);
        return builder;
    }
}
