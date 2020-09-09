/*
   Copyright (c) 2020 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.linkedin.restli.tools.snapshot.gen;

import com.linkedin.pegasus.generator.GeneratorResult;
import com.linkedin.restli.tools.ExporterTestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestPegasusSchemaSnapshotExporter
{
  private final String FS = File.separator;
  private String testDir = System.getProperty("testDir", new File("src/test").getAbsolutePath());
  private String inputDir  = testDir + FS + "pegasus" + FS + "com/linkedin/restli/tools/pegasusSchemaSnapshotTest";
  private String snapshotDir = testDir + FS + "pegasusSchemaSnapshot";

  private File outDir;

  @BeforeMethod
  private void beforeMethod() throws IOException
  {
    outDir = Files.createTempDirectory(this.getClass().getSimpleName() + System.currentTimeMillis()).toFile();
  }

  @AfterMethod
  private void afterMethod() throws IOException
  {
    FileUtils.forceDelete(outDir);
  }

  @Test
  public void testExportSnapshot() throws Exception
  {
    String[] expectedFiles = new String[]
        {
            "BirthInfo.pdl",
            "FullName.pdl",
            "Location.pdl"
        };
    PegasusSchemaSnapshotExporter exporter = new PegasusSchemaSnapshotExporter();
    GeneratorResult result = exporter.export(inputDir, inputDir, outDir);

    Assert.assertEquals(outDir.list().length, expectedFiles.length);
    Assert.assertEquals(result.getModifiedFiles().size(), expectedFiles.length);
    Assert.assertEquals(result.getTargetFiles().size(), expectedFiles.length);

    for (String file : expectedFiles)
    {
      String actualFile = outDir + FS + file;
      String expectedFile = snapshotDir + FS + file;

      ExporterTestUtils.comparePegasusSchemaSnapshotFiles(actualFile, expectedFile);
      Assert.assertTrue(result.getModifiedFiles().contains(new File(actualFile)));
      Assert.assertTrue(result.getTargetFiles().contains(new File(actualFile)));
    }
  }
}