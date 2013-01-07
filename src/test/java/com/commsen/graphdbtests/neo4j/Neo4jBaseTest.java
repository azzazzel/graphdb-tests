
package com.commsen.graphdbtests.neo4j;

import java.io.IOException;

import org.junit.Before;

public abstract class Neo4jBaseTest {

	@Before
	public void clearData()
		throws IOException {

		Neo4jUtil.dropDB();
		Neo4jUtil.createDB();
	}

}
