
package com.commsen.graphdbtests;

import java.util.Collection;
import java.util.LinkedList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public abstract class BaseGraphInsertPerformanceTest extends BaseGraphPerformanceTest {

	public class TestResult {

		long v, e;
		boolean timeout;

		public TestResult(long v, long e, boolean timeout) {

			this.v = v;
			this.e = e;
			this.timeout = timeout;
		}

	}

	protected static enum ModelType {
		VERTECES_ONLY, VERTECES_AND_EDGES, VERTECES_AND_EDGES_BY_ID, EDGES_ONLY, EDGES_ONLY_BY_ID
	}

	protected static long[] executionDocumentsAmounts = new long[] {
		// @formatter:off
		10000, 100000, 1000000
		// @formatter:on
		};

	protected static long[] executionPropertiesAmounts = new long[] {
		// @formatter:off
		0, 1, 10, 50
		// @formatter:on
		};

	protected static ModelType[] executionModels = new ModelType[] {
		ModelType.VERTECES_ONLY, ModelType.VERTECES_AND_EDGES, ModelType.VERTECES_AND_EDGES_BY_ID,
		ModelType.EDGES_ONLY, ModelType.EDGES_ONLY_BY_ID
	};

	protected static long TIMEOUT = 30 * 1000;

	protected static long TIMEOUT_CHECK = 100;

	protected ModelType modelType;

	protected long numberOfDocuments;

	protected long numberOfProperties;

	protected long startTime;

	@Parameters
	public static Collection<Object[]> getParameters() {

		final LinkedList<Object[]> params = new LinkedList<Object[]>();

		for (final Object modelTypes : executionModels) {
			for (final long docs : executionDocumentsAmounts) {
				for (final long properties : executionPropertiesAmounts) {
					params.add(new Object[] {
						docs, properties, modelTypes
					});
				}

			}
		}

		return params;
	}

	public BaseGraphInsertPerformanceTest(
		final long numberOfDocs, final long numberOfProperties, final ModelType modelType) {

		super(TestType.INSERT, "Test inserting " + modelType);

		this.numberOfDocuments = numberOfDocs;
		this.numberOfProperties = numberOfProperties;
		this.modelType = modelType;

	}

	protected void printInsertTimeout(final long v, final long e) {

		super.printInsertTimeout((System.currentTimeMillis() - startTime), numberOfDocuments, numberOfProperties, v, e);
	}

	protected void printInsertTime(final long v, final long e) {

		super.printInsertTime((System.currentTimeMillis() - startTime), numberOfDocuments, numberOfProperties, v, e);
	}

	@Test
	public void addDocuments() {

		startTime = System.currentTimeMillis();

		TestResult result = doAddDocuments();

		if (result.timeout) {
			printInsertTime(result.v, result.e);
		}
		else {
			printInsertTimeout(result.v, result.e);
		}
		
		if (numberOfProperties == executionPropertiesAmounts[executionPropertiesAmounts.length - 1]) {
			printSeparator();
		}

	}

	protected abstract TestResult doAddDocuments();

}
