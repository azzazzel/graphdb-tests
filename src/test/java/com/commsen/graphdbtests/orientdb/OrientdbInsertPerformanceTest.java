
package com.commsen.graphdbtests.orientdb;

import java.util.Collection;
import java.util.LinkedList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;

@RunWith(value = Parameterized.class)
public class OrientdbInsertPerformanceTest extends OrientdbBasePerformanceTest {

	enum ModelType {
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
				if (modelTypes != ModelType.VERTECES_ONLY && num > skipEdgesTestAfter) {
					// do not run with EDGEs when more than 'skipEdgesTestAfter'
					// are to be created as it will take forever!
					continue;
				}
				params.add(new Object[] {
					num, modelTypes
				});
			}
		}

		return params;
	}

	public OrientdbInsertPerformanceTest(long numberOfDocs, ModelType modelType) {

		super(TestType.INSERT, "Test inserting " + modelType);

		this.numberOfDocuments = numberOfDocs;
		this.modelType = modelType;

	}

	@Test
	public void addDocuments() {

		long v = 0, e = 0;

		ORID d1 = null, d2 = null;

		OGraphDatabase db = OrientDbUtil.getDatabase();
		db.declareIntent(new OIntentMassiveInsert());

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
			d1 = db.createVertex().save().getIdentity();
			d2 = db.createVertex().save().getIdentity();
		}

		long t = System.currentTimeMillis();

		ODocument doc = db.createVertex();

		for (int i = 0; i < v; i++) {
			doc.reset();
			doc.setClassName("OGraphVertex");
			doc.save();
			if (i == 0)
				d1 = doc.getIdentity().copy();
			if (i == 1)
				d2 = doc.getIdentity().copy();
		}

		for (int i = 0; i < e; i++) {
			doc = db.createEdge(d1, d2);
			doc.save();
		}

		db.declareIntent(null);

		printInsertTime((System.currentTimeMillis() - t), db.countVertexes(), db.countEdges());

		db.close();
	}

}
