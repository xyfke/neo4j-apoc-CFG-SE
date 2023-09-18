package apoc.multithread;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.util.concurrent.Work;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class Worker implements  Runnable {

    private BlockingQueue<DataflowEdge> processQueue;
    private BlockingQueue<DataflowEdge> results;

    @Context
    GraphDatabaseService db;

    public Worker(BlockingQueue<DataflowEdge> processQueue, BlockingQueue<DataflowEdge> results)
            throws IOException {
        this.processQueue = processQueue;
        this.results = results;
    }

    @Override
    public void run() {
        try (Transaction tx = db.beginTx()) {
            do {
                DataflowEdge item = this.processQueue.take();
                this.processEdge(item);
            } while (true);
        } catch (InterruptedException e) {
            System.out.println("Interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processEdge(DataflowEdge curDataflow) throws InterruptedException {
        Iterable<Relationship> nextRels = DataflowHelper.getNextRels(curDataflow.getCurRelNode());

        for (Relationship nextRel : nextRels) {
            if (!FindDataflow.visitedEdges.contains(nextRel.getId())) {
                processQueue.add(new DataflowEdge(nextRel, curDataflow));
            }
        }
    }


}
