/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.azurefs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.core.fs.FileSystemTestUtils.checkPathEventualExistence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for Azure based
 * file system.
 */
@RunWith(Parameterized.class)
public class AzureFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

	@Parameterized.Parameter
	public String scheme;

	@Parameterized.Parameters(name = "Scheme = {0}")
	public static List<String> parameters() {
		return Arrays.asList("wasb", "wasbs");
	}

	private static final String CONTAINER = System.getenv("ARTIFACTS_AZURE_CONTAINER");
	private static final String ACCOUNT = System.getenv("ARTIFACTS_AZURE_STORAGE_ACCOUNT");
	private static final String ACCESS_KEY = System.getenv("ARTIFACTS_AZURE_ACCESS_KEY");

	private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

	@BeforeClass
	public static void checkCredentialsAndSetup() throws IOException {
		// check whether credentials and container / account details exist
		Assume.assumeTrue("Azure storage account not configured, skipping test...", ACCOUNT != null);
		Assume.assumeTrue("Azure container not configured, skipping test...", CONTAINER != null);
		Assume.assumeTrue("Azure access key not configured, skipping test...", ACCESS_KEY != null);

		// initialize configuration with valid credentials
		final Configuration conf = new Configuration();
		// fs.azure.account.key.youraccount.blob.core.windows.net = ACCESS_KEY
		conf.setString("fs.azure.account.key." + ACCOUNT + ".blob.core.windows.net", ACCESS_KEY);
		FileSystem.initialize(conf);
	}

	@AfterClass
	public static void clearFsConfig() throws IOException {
		FileSystem.initialize(new Configuration());
	}

	@Override
	public FileSystem getFileSystem() throws Exception {
		return getBasePath().getFileSystem();
	}

	@Override
	public Path getBasePath() {
		// wasb(s)://yourcontainer@youraccount.blob.core.windows.net/testDataDir
		String uriString = scheme + "://" + CONTAINER + '@' + ACCOUNT + ".blob.core.windows.net/" + TEST_DATA_DIR;
		return new Path(uriString);
	}

	@Test
	public void testSimpleFileWriteAndRead() throws Exception {
		final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs

		final String testLine = "Hello Upload!";

		final Path path = new Path(getBasePath() + "/test.txt");
		final FileSystem fs = path.getFileSystem();

		try {
			try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.OVERWRITE);
				OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
				writer.write(testLine);
			}

			// just in case, wait for the path to exist
			checkPathEventualExistence(fs, path, true, deadline);

			try (FSDataInputStream in = fs.open(path);
				InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
				BufferedReader reader = new BufferedReader(ir)) {
				String line = reader.readLine();
				assertEquals(testLine, line);
			}
		}
		finally {
			fs.delete(path, false);
		}

		// now file must be gone
		checkPathEventualExistence(fs, path, false, deadline);
	}

	@Test
	public void testDirectoryListing() throws Exception {
		final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs

		final Path directory = new Path(getBasePath() + "/testdir/");
		final FileSystem fs = directory.getFileSystem();

		// directory must not yet exist
		assertFalse(fs.exists(directory));

		try {
			// create directory
			assertTrue(fs.mkdirs(directory));

			checkPathEventualExistence(fs, directory, true, deadline);

			// directory empty
			assertEquals(0, fs.listStatus(directory).length);

			// create some files
			final int numFiles = 3;
			for (int i = 0; i < numFiles; i++) {
				Path file = new Path(directory, "/file-" + i);
				try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.OVERWRITE);
					OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
					writer.write("hello-" + i + "\n");
				}
				// just in case, wait for the file to exist (should then also be reflected in the
				// directory's file list below)
				checkPathEventualExistence(fs, file, true, deadline);
			}

			FileStatus[] files = fs.listStatus(directory);
			assertNotNull(files);
			assertEquals(3, files.length);

			for (FileStatus status : files) {
				assertFalse(status.isDir());
			}

			// now that there are files, the directory must exist
			assertTrue(fs.exists(directory));
		}
		finally {
			// clean up
			fs.delete(directory, true);
		}

		// now directory must be gone
		checkPathEventualExistence(fs, directory, false, deadline);
	}

	@Override
	public FileSystemKind getFileSystemKind() {
		return FileSystemKind.OBJECT_STORE;
	}
}
