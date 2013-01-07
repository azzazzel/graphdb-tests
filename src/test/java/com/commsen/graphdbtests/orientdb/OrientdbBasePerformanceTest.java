
package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import org.junit.Before;

import com.commsen.graphdbtests.BaseGraphPerformanceTest;

public abstract class OrientdbBasePerformanceTest extends BaseGraphPerformanceTest {

	
	public OrientdbBasePerformanceTest(TestType testType, String header) {

		super(testType, header);
	}


	@Before
	public void clearData()
		throws IOException {
		
			OrientDbUtil.dropDB();
			OrientDbUtil.createDB();
		}



}
