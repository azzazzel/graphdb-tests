
package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

public class DropDatabaseWithIndexTest {

	private static OGraphDatabasePool dbConnectionPool = OGraphDatabasePool.global();

	@Before
	public void init()
		throws IOException {

		dropDB();
		createDB();
	}

	@After
	public void cleanup()
		throws IOException {

		dropDB();
	}

	@Test
	public void createVertex1()
		throws IOException {

		OGraphDatabase connection = OrientDbUtil.getDatabase();
		ODocument doc = connection.createVertex();
		doc.field("name", "test");
		connection.save(doc);

	}

	@Test
	public void createVertex2() {

		ODocument doc = OrientDbUtil.getDatabase().createVertex();
		doc.field("name", "test");
		doc.save();
	}

	private static void createDB()
		throws IOException {

		OrientDbUtil.createDB();

		OGraphDatabase database = OrientDbUtil.getDatabase();
		database.command(new OCommandSQL("CREATE PROPERTY V.name STRING")).execute();
		database.command(new OCommandSQL("CREATE INDEX V.name UNIQUE")).execute();

	}

	public static void dropDB()
		throws IOException {

		if (OrientDbUtil.dbUrl.startsWith("remote")) {
			OServerAdmin server = new OServerAdmin(OrientDbUtil.dbUrl).connect(OrientDbUtil.dbUser, OrientDbUtil.dbPassword);
			if (server.existsDatabase()) {
				server.dropDatabase();
			}
			server.close();
		}
		else {
			OGraphDatabase database = new OGraphDatabase(OrientDbUtil.dbUrl);
			if (database.exists()) {
				if (database.isClosed()) {
					database.open(OrientDbUtil.dbUser, OrientDbUtil.dbPassword);
				}

				database.drop();
			}
			database.close();
		}
	}

}
