
package com.commsen.graphdbtests.neo4j;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import com.commsen.graphdbtests.BaseGraphInsertPerformanceTest;
import com.commsen.graphdbtests.BaseGraphInsertPerformanceTest.TestResult;

public class Neo4jInsertPerformanceTest extends BaseGraphInsertPerformanceTest {

	private static enum RelTypes
		implements RelationshipType {
		RELATES_TO
	}

	public Neo4jInsertPerformanceTest(long numberOfDocs, long numberOfProperties, ModelType modelType) {

		super(numberOfDocs, numberOfProperties, modelType);
		// TODO Auto-generated constructor stub
	}

	protected Relationship createRelationShip(Node node1, Node node2) {

		Relationship r = node1.createRelationshipTo(node2, RelTypes.RELATES_TO);
		for (int i = 0; i < numberOfProperties; i++) {
			r.setProperty("property" + i, "value" + i);
		}
		return r;
	}

	protected Node createNode(final GraphDatabaseService db) {

		Node n = db.createNode();
		for (int i = 0; i < numberOfProperties; i++) {
			n.setProperty("property" + i, "value" + i);
		}
		return n;
	}

	@Before
	public void clearData()
		throws IOException {

		Neo4jUtil.dropDB();
		Neo4jUtil.createDB();
	}

	private long getNodes() {

		final ExecutionEngine engine = new ExecutionEngine(Neo4jUtil.getDatabase());
		final ExecutionResult result = engine.execute("START n=node(*) RETURN count(n) AS c");
		return (Long) result.columnAs("c").next();
	}

	private long getRelations() {

		final ExecutionEngine engine = new ExecutionEngine(Neo4jUtil.getDatabase());
		final ExecutionResult result = engine.execute("START r=relationship(*) RETURN count(r) AS c");
		return (Long) result.columnAs("c").next();
	}

	@Override
	protected TestResult doAddDocuments() {
		long v = 0, e = 0;

		Node node1 = null, node2 = null;
		Long id1 = null, id2 = null;

		final long startTime = System.currentTimeMillis();

		final GraphDatabaseService db = Neo4jUtil.getDatabase();
		final Transaction tx = db.beginTx();

		boolean referenceById = false;
		if (modelType == ModelType.EDGES_ONLY_BY_ID || modelType == ModelType.VERTECES_AND_EDGES_BY_ID) {
			referenceById = true;
		}

		try {
			switch (modelType) {

			case VERTECES_AND_EDGES:
			case VERTECES_AND_EDGES_BY_ID:
				v = numberOfDocuments / 2;
				e = numberOfDocuments - v;
				break;

			case VERTECES_ONLY:
				v = numberOfDocuments;
				break;

			case EDGES_ONLY:
			case EDGES_ONLY_BY_ID:
				e = numberOfDocuments - 2;
				if (referenceById) {
					id1 = createNode(db).getId();
					id2 = createNode(db).getId();
				}
				else {
					node1 = createNode(db);
					node2 = createNode(db);
				}
			}

			for (int i = 0; i < v; i++) {

				if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
					return new TestResult(i, 0, true);
				}

				final Node node = createNode(db);
				if (i == 0)
					if (referenceById) {
						id1 = node.getId();
					}
					else {
						node1 = node;
					}
				if (i == 1)
					if (referenceById) {
						id2 = node.getId();
					}
					else {
						node2 = node;
					}
			}

			for (int i = 0; i < e; i++) {

				if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
					return new TestResult(v, i, true);
				}

				if (referenceById) {
					node1 = db.getNodeById(id1);
					node2 = db.getNodeById(id2);
				}
				createRelationShip(node1, node2);
			}
			tx.success();
		}
		finally {
			tx.finish();
		}
		
		return new TestResult(getNodes(), getRelations(), true);
		
	}
}
