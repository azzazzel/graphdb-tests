
package com.commsen.graphdbtests.neo4j;

import java.io.IOException;

import org.junit.Before;

import com.commsen.graphdbtests.BaseGraphPerformanceTest;

public abstract class Neo4jBasePerformanceTest extends BaseGraphPerformanceTest {

	public Neo4jBasePerformanceTest(TestType testType, String header) {

		super(testType, header);
	}

	@Before
	public void clearData()
		throws IOException {

		Neo4jUtil.dropDB();
		Neo4jUtil.createDB();
	}

}
