/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.conf;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Annotation processor to generate config fragments from Config annotations.
 */
@SupportedAnnotationTypes("org.apache.hadoop.hdds.conf.ConfigGroup")
public class ConfigFileGenerator extends AbstractProcessor {

  public static final String OUTPUT_FILE_NAME = "ozone-default-generated.xml";

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
      RoundEnvironment roundEnv) {
    if (roundEnv.processingOver()) {
      return false;
    }

    Filer filer = processingEnv.getFiler();

    try {

      //load existing generated config (if exists)
      ConfigFileAppender appender = new ConfigFileAppender();
      try (InputStream input = filer
          .getResource(StandardLocation.CLASS_OUTPUT, "",
              OUTPUT_FILE_NAME).openInputStream()) {
        appender.load(input);
      } catch (FileNotFoundException ex) {
        appender.init();
      }

      Set<? extends Element> annotatedElements =
          roundEnv.getElementsAnnotatedWith(ConfigGroup.class);
      for (Element annotatedElement : annotatedElements) {
        TypeElement configGroup = (TypeElement) annotatedElement;

        //check if any of the setters are annotated with @Config
        for (Element element : configGroup.getEnclosedElements()) {
          if (element.getKind() == ElementKind.METHOD) {
            processingEnv.getMessager()
                .printMessage(Kind.WARNING, element.getSimpleName().toString());
            if (element.getSimpleName().toString().startsWith("set")
                && element.getAnnotation(Config.class) != null) {

              //update the ozone-site-generated.xml
              Config configAnnotation = element.getAnnotation(Config.class);
              ConfigGroup configGroupAnnotation =
                  configGroup.getAnnotation(ConfigGroup.class);

              String key = configGroupAnnotation.prefix() + "."
                  + configAnnotation.key();

              appender.addConfig(key,
                  configAnnotation.defaultValue(),
                  configAnnotation.description(),
                  configAnnotation.tags());
            }
          }

        }
        FileObject resource = filer
            .createResource(StandardLocation.CLASS_OUTPUT, "",
                OUTPUT_FILE_NAME);

        try (Writer writer = new OutputStreamWriter(
            resource.openOutputStream(), StandardCharsets.UTF_8)) {
          appender.write(writer);
        }
      }
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Kind.ERROR,
          "Can't generate the config file from annotation: " + e.getMessage());
    }
    return false;
  }


}
