
package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import org.junit.Before;

public abstract class OrientdbBaseTest {

	@Before
	public void clearData()
		throws IOException {

		OrientDbUtil.dropDB();
		OrientDbUtil.createDB();
	}

}
