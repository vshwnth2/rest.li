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

import com.linkedin.data.schema.AbstractSchemaEncoder;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaResolver;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.SchemaToPdlEncoder;
import com.linkedin.data.schema.grammar.PdlSchemaParser;
import com.linkedin.data.schema.resolver.MultiFormatDataSchemaResolver;
import com.linkedin.pegasus.generator.GeneratorResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;


/**
 * PegasusSchemaSnapshotExporter, generating pegasus schema snapshot(.pdl) files
 *
 * @author Yingjie Bi
 */
public class PegasusSchemaSnapshotExporter
{
  private static final String PEGASUS_SCHEMA_SNAPSHOT_SUFFIX = ".pdl";

  private static final String PDL = "pdl";

  /**
   * Generate pegasus schema snapshot(pegasusSchemaSnapshot.pdl) files to the provided output directory
   * based on the given input pegasus schemas.
   *
   * @param resolverPath schema resolver path
   * @param inputPath input files directory
   * @param outputDir output files directory
   * @return GeneratorResult
   * @throws IOException
   */
  public GeneratorResult export(String resolverPath, String inputPath, File outputDir) throws IOException
  {
    List<DataSchema> dataSchemas = parseDataSchema(resolverPath, inputPath);
    pegasusSchemaSnapshotResult result = new pegasusSchemaSnapshotResult();
    for (DataSchema dataSchema : dataSchemas)
    {
     File file = writeSnapshotFile(outputDir, ((NamedDataSchema) dataSchema).getFullName(), dataSchema);
     result.addModifiedFile(file);
     result.addTargetFile(file);
    }
    return result;
  }

  private static List<DataSchema> parseDataSchema(String resolverPath, String inputPath)
      throws RuntimeException, FileNotFoundException
  {
    Iterator<File> iterator = FileUtils.iterateFiles(new File(inputPath), new String[]{PDL}, true);
    List<DataSchema> schemas = new ArrayList<>();
    while (iterator.hasNext())
    {
      DataSchemaResolver resolver = MultiFormatDataSchemaResolver.withBuiltinFormats(resolverPath);
      File inputFile = iterator.next();
      PdlSchemaParser parser = new PdlSchemaParser(resolver);
      parser.parse(new FileInputStream(inputFile));

      if (parser.hasError())
      {
        throw new RuntimeException(parser.errorMessage());
      }

      List<DataSchema> topLevelDataSchemas = parser.topLevelDataSchemas();
      if (topLevelDataSchemas == null || topLevelDataSchemas.isEmpty() || topLevelDataSchemas.size() > 1)
      {
        throw new RuntimeException("Could not parse schema : " + inputFile.getAbsolutePath());
      }
      DataSchema topLevelDataSchema = topLevelDataSchemas.get(0);
      if (!(topLevelDataSchema instanceof NamedDataSchema))
      {
        throw new RuntimeException("Invalid schema : " + inputFile.getAbsolutePath() + ", the schema is not a named schema.");
      }
      schemas.add(topLevelDataSchema);
    }
    return schemas;
  }

  private static File writeSnapshotFile(File outputDir, String fileName, DataSchema dataSchema) throws IOException
  {
    StringWriter stringWriter = new StringWriter();
    SchemaToPdlEncoder schemaToPdlEncoder = new SchemaToPdlEncoder(stringWriter);
    schemaToPdlEncoder.setTypeReferenceFormat(AbstractSchemaEncoder.TypeReferenceFormat.DENORMALIZE);
    schemaToPdlEncoder.encode(dataSchema);

    File generatedSnapshotFile = new File(outputDir, fileName + PEGASUS_SCHEMA_SNAPSHOT_SUFFIX);

    if (generatedSnapshotFile.exists())
    {
      if (!generatedSnapshotFile.delete())
      {
        throw new IOException(generatedSnapshotFile + ": Can't delete previous version");
      }
    }

    try (FileOutputStream out = new FileOutputStream(generatedSnapshotFile))
    {
      out.write(stringWriter.toString().getBytes());
    }
    return generatedSnapshotFile;
  }

  private static class pegasusSchemaSnapshotResult implements GeneratorResult
  {
    private List<File> targetFiles = new ArrayList<File>();
    private List<File> modifiedFiles = new ArrayList<File>();

    public void addTargetFile(File file)
    {
      targetFiles.add(file);
    }

    public void addModifiedFile(File file)
    {
      modifiedFiles.add(file);
    }

    @Override
    public Collection<File> getSourceFiles()
    {
      throw new UnsupportedOperationException("getSourceFiles is not supported for the PegasusSchemaSnapshotExporter");
    }

    @Override
    public Collection<File> getTargetFiles()
    {
      return targetFiles;
    }

    @Override
    public Collection<File> getModifiedFiles()
    {
      return modifiedFiles;
    }
  }
}