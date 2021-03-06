/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.LazyInitSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class LazyInitHadoopInputFile implements InputFile {

  private final Configuration conf;
  private FileStatus stat;

  public static LazyInitHadoopInputFile fromPath(Path path, Configuration conf)
      throws IOException {
    return new LazyInitHadoopInputFile(path, conf);
  }

  private LazyInitHadoopInputFile(Path path, Configuration conf) throws IOException {
    this.conf = conf;
    this.stat = path.getFileSystem(conf).getFileStatus(path);
  }

  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public long getLength() {
    return stat.getLen();
  }

  @Override
  public SeekableInputStream newStream() {
    return LazyInitSeekableInputStream.wrap(stat.getPath(), conf);
  }

  @Override
  public String toString() {
    return stat.getPath().toString();
  }
}
