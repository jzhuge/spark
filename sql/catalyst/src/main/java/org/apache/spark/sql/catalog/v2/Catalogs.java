/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalog.v2;

import org.apache.spark.SparkException;
import org.apache.spark.annotation.Private;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.Utils;

import java.util.Map;
import java.util.stream.Collectors;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

@Private
public class Catalogs {
  private Catalogs() {
  }

  private static String classKey(String catalogName) {
    return "spark.sql.catalog." + catalogName;
  }

  private static String optionKeyPrefix(String catalogName) {
    return "spark.sql.catalog." + catalogName + ".";
  }

  private static boolean isOptionKey(String keyName, String catalogName) {
    return keyName.startsWith(optionKeyPrefix(catalogName));
  }

  private static String optionKeyToName(String keyName, String catalogName) {
    return keyName.substring(optionKeyPrefix(catalogName).length());
  }

  /**
   * Load and configure a catalog by name.
   * <p>
   * This loads, instantiates, and initializes the catalog plugin for each call; it does not cache
   * or reuse instances.
   *
   * @param name a String catalog name
   * @param conf a SQLConf
   * @return an initialized CatalogPlugin
   * @throws CatalogNotFoundException if the plugin class cannot be found
   * @throws SparkException if the plugin class cannot be instantiated
   */
  public static CatalogPlugin load(String name, SQLConf conf)
      throws CatalogNotFoundException, SparkException {
    String pluginClassName = conf.getConfString(classKey(name), null);
    if (pluginClassName == null) {
      throw new CatalogNotFoundException(String.format(
          "Catalog '%s' plugin class not found: %s is not defined", name, classKey(name)));
    }

    ClassLoader loader = Utils.getContextOrSparkClassLoader();

    try {
      Class<?> pluginClass = loader.loadClass(pluginClassName);

      if (!CatalogPlugin.class.isAssignableFrom(pluginClass)) {
        throw new SparkException(String.format(
            "Plugin class for catalog '%s' does not implement CatalogPlugin: %s",
            name, pluginClassName));
      }

      CatalogPlugin plugin = CatalogPlugin.class.cast(pluginClass.newInstance());

      plugin.initialize(name, catalogOptions(name, conf));

      return plugin;

    } catch (ClassNotFoundException e) {
      throw new SparkException(String.format(
          "Cannot find catalog plugin class for catalog '%s': %s", name, pluginClassName));

    } catch (IllegalAccessException e) {
      throw new SparkException(String.format(
          "Failed to call public no-arg constructor for catalog '%s': %s", name, pluginClassName),
          e);

    } catch (InstantiationException e) {
      throw new SparkException(String.format(
          "Failed while instantiating plugin for catalog '%s': %s", name, pluginClassName),
          e.getCause());
    }
  }

  /**
   * Extracts a named catalog's configuration from a SQLConf.
   *
   * @param name a catalog name
   * @param conf a SQLConf
   * @return a case insensitive string map of options starting with spark.sql.catalog.(name).
   */
  private static CaseInsensitiveStringMap catalogOptions(String name, SQLConf conf) {
    Map<String, String> options =
        mapAsJavaMapConverter(conf.getAllConfs()).asJava().entrySet().stream()
            .filter(e -> isOptionKey(e.getKey(), name))
            .collect(Collectors.toMap(
                e -> optionKeyToName(e.getKey(), name),
                e-> e.getValue()));
    return new CaseInsensitiveStringMap(options);
  }

  /**
   * Add a catalog.
   *
   * @param name a String catalog name
   * @param options catalog options
   * @param conf a SQLConf
   */
  public static void add(
      String name,
      String pluginClassName,
      CaseInsensitiveStringMap options,
      SQLConf conf) {
    options.entrySet().stream()
        .forEach(e -> conf.setConfString(optionKeyPrefix(name) + e.getKey(), e.getValue()));
    conf.setConfString(classKey(name), pluginClassName);
  }

  /**
   * Remove a catalog.
   *
   * @param name a String catalog name
   * @param conf a SQLConf
   */
  public static void remove(String name, SQLConf conf) {
    conf.unsetConf(classKey(name));
    mapAsJavaMapConverter(conf.getAllConfs()).asJava().keySet().stream()
        .filter(key -> isOptionKey(key, name))
        .forEach(conf::unsetConf);
  }
}
