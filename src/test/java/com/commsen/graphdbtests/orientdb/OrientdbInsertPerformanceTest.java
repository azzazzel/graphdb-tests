
package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.commsen.graphdbtests.BaseGraphInsertPerformanceTest;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OrientdbInsertPerformanceTest extends BaseGraphInsertPerformanceTest {

	public OrientdbInsertPerformanceTest(long numberOfDocs, long numberOfProperties, ModelType modelType) {

		super(numberOfDocs, numberOfProperties, modelType);
		// TODO Auto-generated constructor stub
	}


	protected void createVertex(ODocument doc) {

		doc.reset();
		doc.setClassName("OGraphVertex");
		for (int i = 0; i < numberOfProperties; i++) {
			doc.field("property" + i, "value" + i);
		}
		doc.save();
	}

	protected ODocument createEdge(ODocument doc1, ODocument doc2, final OGraphDatabase db) {

		ODocument doc = db.createEdge(doc1, doc2);
		for (int i = 0; i < numberOfProperties; i++) {
			doc.field("property" + i, "value" + i);
		}
		return doc;
	}

	protected ODocument createEdge(ORID id1, ORID id2, final OGraphDatabase db) {

		ODocument doc = db.createEdge(id1, id2);
		for (int i = 0; i < numberOfProperties; i++) {
			doc.field("property" + i, "value" + i);
		}
		return doc;
	}

	@Before
	public void clearData()
		throws IOException {

		OrientDbUtil.dropDB();
		OrientDbUtil.createDB();
	}


	@Override
	protected TestResult doAddDocuments() {
		long v = 0, e = 0;

		ODocument doc1 = null, doc2 = null;
		ORID id1 = null, id2 = null;

		final OGraphDatabase db = OrientDbUtil.getDatabase();
		try {
			db.declareIntent(new OIntentMassiveInsert());

			boolean referenceById = false;
			if (modelType == ModelType.EDGES_ONLY_BY_ID || modelType == ModelType.VERTECES_AND_EDGES_BY_ID) {
				referenceById = true;
			}

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
					id1 = db.createVertex().save().getIdentity();
					id2 = db.createVertex().save().getIdentity();
				}
				else {
					doc1 = db.createVertex().save();
					doc2 = db.createVertex().save();
				}
			}


			ODocument doc = db.createVertex();

			for (int i = 0; i < v; i++) {

				if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
					return new TestResult(i, 0, true);
				}

				createVertex(doc);
				if (i == 0) {
					if (referenceById) {
						id1 = doc.getIdentity().copy();
					}
					else {
						doc1 = doc.copy();
					}
				}
				if (i == 1) {
					if (referenceById) {
						id2 = doc.getIdentity().copy();
					}
					else {
						doc2 = doc.copy();
					}
				}
			}

			for (int i = 0; i < e; i++) {

				if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
					return new TestResult(v, i, true);
				}

				if (referenceById) {
					doc = createEdge(id1, id2, db);
				}
				else {
					doc = createEdge(doc1, doc2, db);
				}
				doc.save();
			}

			db.declareIntent(null);

		}
		finally {
			db.close();
		}
		
		return new TestResult(db.countVertexes(), db.countEdges(), false);
	}

}
