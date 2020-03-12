/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.internal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat

object HiveSerDeDef {

  def getDefaultStorage(conf: SQLConf): CatalogStorageFormat = {
    // To respect hive-site.xml, it peeks Hadoop configuration from existing Spark session,
    // as an easy workaround. See SPARK-27555.
    val defaultFormatKey = "hive.default.fileformat"
    val defaultValue = {
      val defaultFormatValue = "textfile"
      SparkSession.getActiveSession.map { session =>
        session.sessionState.newHadoopConf().get(defaultFormatKey, defaultFormatValue)
      }.getOrElse(defaultFormatValue)
    }
    val defaultStorageType = conf.getConfString("hive.default.fileformat", defaultValue)
    val defaultHiveSerde = HiveSerDe.sourceToSerDe(defaultStorageType)
    CatalogStorageFormat.empty.copy(
      inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
        .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
      outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
        .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
      serde = defaultHiveSerde.flatMap(_.serde)
        .orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
  }
}
