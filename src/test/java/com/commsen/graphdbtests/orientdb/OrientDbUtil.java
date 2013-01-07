
package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;

public class OrientDbUtil {

	public static String dbUrl = "local:/tmp/graphdb_tests_orient";

	public static String dbUser = "admin";

	public static String dbPassword = "admin";

	public static OGraphDatabasePool dbConnectionPool = OGraphDatabasePool.global();

	public static OGraphDatabase getDatabase() {

		return OrientDbUtil.dbConnectionPool.acquire(dbUrl, dbUser, dbPassword);
	}

	public static void createDB()
		throws IOException {

		if (dbUrl.startsWith("remote")) {
			OServerAdmin server = new OServerAdmin(dbUrl).connect(dbUser, dbPassword);
			if (!server.existsDatabase()) {
				server.createDatabase("graph", "local");
			}
			server.close();
		}
		else {
			OGraphDatabase database = new OGraphDatabase(dbUrl);
			if (!database.exists()) {
				database.create();
			}
			database.close();
		}

	}

	public static void dropDB()
		throws IOException {

		if (dbUrl.startsWith("remote")) {
			OServerAdmin server = new OServerAdmin(dbUrl).connect(dbUser, dbPassword);
			if (server.existsDatabase()) {
				server.dropDatabase();
			}
			server.close();
		}
		else {
			OGraphDatabase database = new OGraphDatabase(dbUrl);
			if (database.exists()) {
				if (database.isClosed()) {
					database.open(dbUser, dbPassword);
				}
				OIndexManager indexManager = database.getMetadata().getIndexManager();
				for (@SuppressWarnings("rawtypes") OIndex index : indexManager.getIndexes()) {
					index.delete();
				}
				database.drop();
			}
			database.close();
		}
	}

}
