
package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.exception.OTransactionException;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class NestedTransactionBlueprintsTest {

	@Before
	public void init()
		throws IOException {

		OrientDbUtil.dropDB();
		OrientDbUtil.createDB();
	}

	@After
	public void cleanup()
		throws IOException {

		OrientDbUtil.dropDB();
	}

	
	@Test
	public void testInnerRollback() {
		saveOuter(false, true);
		printDocuments("rollback inner only");
	}

	@Test
	public void testOuterRollback() {
		saveOuter(true, false);
		printDocuments("rollback outer only");
	}

	@Test
	public void testNoRollback() {
		saveOuter(false, false);
		printDocuments("no rollbacks");
	}
	
	@Test
	public void testRollbackBoth() {
		saveOuter(true, true);
		printDocuments("rollback both");
	}

	
	private void saveOuter(boolean rollbackOuter, boolean rollbackInner) {
		TransactionalGraph connection = new OrientGraph(OrientDbUtil.dbUrl);
		try {
			Vertex doc = connection.addVertex(null);
			doc.setProperty("method", "saveOuter");
			saveInner(rollbackInner);
			if (rollbackOuter) {
				connection.rollback();
			} else {
				connection.commit();
			}
		} catch (OTransactionException e) {
			e.printStackTrace();
		} finally {
			connection.shutdown();
		}
	}

	private void saveInner(boolean rollback) {
		TransactionalGraph connection = new OrientGraph(OrientDbUtil.dbUrl);
		try {
			Vertex doc = connection.addVertex(null);
			doc.setProperty("method", "saveInner");
			if (rollback) {
				connection.rollback();
			} else {
				connection.commit();
			}
		} catch (OTransactionException e) {
			e.printStackTrace();
		} finally {
			connection.shutdown();
		}
	}

	private void printDocuments(String message) {
		Graph connection = new OrientGraph(OrientDbUtil.dbUrl);
		for (Vertex v : connection.getVertices()) {
			System.out.println("|" + message + "|--> " + v + " -> method: " + v.getProperty("method"));
		}
		connection.shutdown();
	}
	
	

}
