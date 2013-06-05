package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class NestedTransactionTest {

	@Before
	public void init() throws IOException {

		OrientDbUtil.dropDB();
		OrientDbUtil.createDB();
	}

	@After
	public void cleanup() throws IOException {

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
		OGraphDatabase connection = OrientDbUtil.getDatabase();
		if (!connection.getMetadata().getSchema().existsClass("TEST")) {
			connection.getMetadata().getSchema().createClass("TEST");
		}
		try {
			connection.begin();
			ODocument doc = new ODocument("TEST");
			doc.field("method", "saveOuter");
			doc.save();
			saveInner(rollbackInner);
			if (rollbackOuter) {
				connection.rollback();
			} else {
				connection.commit();
			}
		} catch (OTransactionException e) {
			e.printStackTrace();
		} finally {
			connection.close();
		}
	}

	private void saveInner(boolean rollback) {
		OGraphDatabase connection = OrientDbUtil.getDatabase();
		try {
			connection.begin();
			ODocument doc = new ODocument("TEST");
			doc.field("method", "saveInner");
			doc.save();
			if (rollback) {
				connection.rollback();
			} else {
				connection.commit();
			}
		} catch (OTransactionException e) {
			e.printStackTrace();
		} finally {
			connection.close();
		}
	}

	private void printDocuments(String message) {
		OGraphDatabase connection = OrientDbUtil.getDatabase();
		for (ODocument d : connection.browseClass("TEST")) {
			System.out.println("|" + message + "|--> " + d);
		}

		connection.close();
	}

}
