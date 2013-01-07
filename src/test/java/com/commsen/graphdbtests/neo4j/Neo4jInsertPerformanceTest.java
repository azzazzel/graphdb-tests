
package com.commsen.graphdbtests.neo4j;

import java.util.Collection;
import java.util.LinkedList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

@RunWith(value = Parameterized.class)
public class Neo4jInsertPerformanceTest extends Neo4jBasePerformanceTest {

	private static enum RelTypes
		implements RelationshipType {
		RELATES_TO
	}

	private static enum ModelType {
		VERTECES_ONLY, VERTECES_AND_EDGES, EDGES_ONLY
	}

	protected ModelType modelType;

	protected long numberOfDocuments;

	protected static long skipEdgesTestAfter = 25000;

	@Parameters
	public static Collection<Object[]> getParameters() {

		long[] verteces = new long[] {
			// @formatter:off
			1000, 10000, 25000, 50000, 100000, 500000, 1000000
			// @formatter:on
			};

		Object[] types = new Object[] {
			ModelType.VERTECES_ONLY, ModelType.VERTECES_AND_EDGES, ModelType.EDGES_ONLY
			};

		LinkedList<Object[]> params = new LinkedList<Object[]>();

		for (Object modelTypes : types) {
			for (long num : verteces) {
				params.add(new Object[] {
					num, modelTypes
				});
			}
		}

		return params;
	}

	public Neo4jInsertPerformanceTest(long numberOfDocs, ModelType modelType) {

		super(TestType.INSERT, "Test inserting " + modelType);

		this.numberOfDocuments = numberOfDocs;
		this.modelType = modelType;

	}

	@Test
	public void addDocuments() {

		long v = 0, e = 0;

		Node node1 = null, node2 = null;

		long t = System.currentTimeMillis();

		GraphDatabaseService db = Neo4jUtil.getDatabase();
		Transaction tx = db.beginTx();
		try {
			switch (modelType) {

			case VERTECES_AND_EDGES:
				v = numberOfDocuments / 2;
				e = numberOfDocuments - v;
				break;

			case VERTECES_ONLY:
				v = numberOfDocuments;
				break;

			case EDGES_ONLY:
				e = numberOfDocuments - 2;
				node1 = db.createNode();
				node2 = db.createNode();
			}

			for (int i = 0; i < v; i++) {
				Node node = db.createNode();
				if (i == 0)
					node1 = node;
				if (i == 1)
					node2 = node;
			}

			for (int i = 0; i < e; i++) {
				node1.createRelationshipTo(node2, RelTypes.RELATES_TO);
			}
			tx.success();
		}
		finally {
			tx.finish();
		}

		printInsertTime((System.currentTimeMillis() - t), getNodes(), getRelations());

	}

	private long getNodes() {
		ExecutionEngine engine = new ExecutionEngine(Neo4jUtil.getDatabase());
		ExecutionResult result = engine.execute("START n=node(*) RETURN count(n) AS c");
		return (Long) result.columnAs("c").next();
	}

	private long getRelations() {
		ExecutionEngine engine = new ExecutionEngine(Neo4jUtil.getDatabase());
		ExecutionResult result = engine.execute("START r=relationship(*) RETURN count(r) AS c");
		return (Long) result.columnAs("c").next();
	}
}
