/**
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

/*
 * Post-processes the CycloneDX SBOM produced by cyclonedx-maven-plugin
 * (target/bom.xml, spec 1.6) into a CycloneDX 1.7 document:
 *
 *  - ordinary external dependencies get an explicit `isExternal="true"`
 *    attribute; dependencies embedded into this jar by maven-shade-plugin
 *    rely on the schema default of `false`;
 *  - hash lists are trimmed to the SHA-256 entry to keep the document small;
 *  - when the module applies package relocation, each embedded component is a
 *    derivative of the original artifact (shade rewrites the classes), so it
 *    gets a `pedigree` with one ancestor describing the original standalone
 *    artifact and the hash computed by the plugin moves onto that ancestor:
 *    the checksum identifies the original artifact, not the rewritten copy.
 *    The purl and bom-ref stay on the embedded component so that security
 *    scanners can still match it against vulnerability databases;
 *  - when the module shades without relocations, the embedded classes are
 *    verbatim copies, so the component keeps its hash and gets a `pedigree`
 *    with only a note documenting the embedding.
 *
 * Relocation is detected per module, not per artifact: when any relocation is
 * configured, shade also rewrites references inside classes whose own package
 * is not relocated (e.g. the org.apache.hadoop classes in hadoop-client-api),
 * so every embedded artifact is potentially modified.
 *
 * The shaded artifact set is derived from the effective Maven model (the
 * maven-shade-plugin `artifactSet` of this module, profile-resolved), so it
 * cannot drift from the shade configuration.  With `-DskipShade` the shade
 * profiles deactivate, the plugin disappears from the effective model and
 * every dependency is classified external, matching the jar actually built.
 *
 * Runs in the `verify` phase via gmavenplus-plugin (bindings: project,
 * session, log), i.e. after maven-shade-plugin but before install/deploy.
 * Consequence: a plain `mvn package` leaves the unprocessed 1.6 document in
 * target/bom.xml; it is rewritten before any artifact leaves the machine.
 *
 * Known approximation: with `<minimizeJar>` (hadoop-gcp) an included artifact
 * may contribute only part of its classes, but it is still declared shaded;
 * the declaration reflects the shade configuration.
 */

import java.util.regex.Pattern
import org.apache.maven.plugin.MojoExecutionException
import org.apache.maven.plugin.MojoFailureException
import org.cyclonedx.Version
import org.cyclonedx.generators.BomGeneratorFactory
import org.cyclonedx.model.Ancestors
import org.cyclonedx.model.Bom
import org.cyclonedx.model.Component
import org.cyclonedx.model.Pedigree
import org.cyclonedx.parsers.XmlParser

String skip = session.userProperties.getProperty('cyclonedx.skip') ?:
    project.properties.getProperty('cyclonedx.skip')
if (Boolean.parseBoolean(skip)) {
  log.info('SBOM post-processing skipped (cyclonedx.skip)')
  return
}

File bomFile = new File(project.build.directory, 'bom.xml')
if (!bomFile.isFile()) {
  log.info("SBOM post-processing: ${bomFile} not present, nothing to do")
  return
}

// Ant-style glob on a single token, as maven-shade-plugin's SelectorUtils
// applies it to each coordinate field.
def globToPattern = { String glob ->
  StringBuilder re = new StringBuilder()
  glob.each { ch ->
    re.append(ch == '*' ? '.*' : ch == '?' ? '.' : Pattern.quote(ch))
  }
  Pattern.compile(re.toString())
}
// maven-shade-plugin 3.6.0 ArtifactId pattern rules:
// g | g:a | g:a:classifier | g:a:type:classifier, absent fields default to *.
def parsePattern = { String p ->
  String[] t = p.split(':', -1)
  [g: globToPattern(t[0]),
   a: globToPattern(t.length > 1 ? t[1] : '*'),
   t: globToPattern(t.length > 3 ? t[2] : '*'),
   c: globToPattern(t.length > 3 ? t[3] : (t.length > 2 ? t[2] : '*'))]
}
def selects = { patterns, artifact ->
  patterns.any {
    it.g.matcher(artifact.groupId).matches() &&
    it.a.matcher(artifact.artifactId).matches() &&
    it.t.matcher(artifact.type ?: '').matches() &&
    it.c.matcher(artifact.classifier ?: '').matches()
  }
}

// Derive the set of shaded artifacts from the effective model.
def shadePlugin = project.buildPlugins.find {
  it.groupId == 'org.apache.maven.plugins' && it.artifactId == 'maven-shade-plugin'
}
def shadeExecution = shadePlugin?.executions?.find { it.goals.contains('shade') }
Set<String> shadedGavs = [] as Set
boolean hasRelocations = false
if (shadeExecution != null) {
  def conf = shadeExecution.configuration ?: shadePlugin.configuration
  if (conf?.getChild('shadedGroupFilter') != null) {
    throw new MojoExecutionException(
        'SBOM post-processing: maven-shade-plugin shadedGroupFilter is not supported')
  }
  hasRelocations = (conf?.getChild('relocations')?.childCount ?: 0) > 0
  def artifactSet = conf?.getChild('artifactSet')
  def includes = (artifactSet?.getChild('includes')?.getChildren('include') ?: [])
      .collect { parsePattern(it.value.trim()) }
  def excludes = (artifactSet?.getChild('excludes')?.getChildren('exclude') ?: [])
      .collect { parsePattern(it.value.trim()) }
  project.artifacts.each { a ->
    // ShadeMojo resolves ResolutionScope.RUNTIME (compile + runtime) and skips
    // pom-type artifacts.  This script runs under TEST resolution, so filter.
    if (!(a.scope in ['compile', 'runtime']) || a.type == 'pom') {
      return
    }
    if ((includes.isEmpty() || selects(includes, a)) && !selects(excludes, a)) {
      shadedGavs << "${a.groupId}:${a.artifactId}:${a.baseVersion ?: a.version}".toString()
    }
  }
  if (shadedGavs.isEmpty()) {
    throw new MojoFailureException(
        'SBOM post-processing: maven-shade-plugin is active but no dependency was classified as shaded')
  }
}

// Keep only the SHA-256 entry of a hash list (null when there is none).
def sha256Only = { hashes ->
  def filtered = hashes?.findAll { it.algorithm == 'SHA-256' }
  filtered ? filtered : null
}

Bom bom = new XmlParser().parse(bomFile)
bom.metadata?.toolChoice?.components?.each { it.hashes = sha256Only(it.hashes) }
Set<String> unmatched = new HashSet<>(shadedGavs)
int shadedCount = 0
bom.components?.each { Component c ->
  String gav = "${c.group}:${c.name}:${c.version}".toString()
  if (shadedGavs.contains(gav)) {
    unmatched.remove(gav)
    shadedCount++
    // isExternal defaults to false, so it is omitted on embedded components
    if (hasRelocations) {
      Component ancestor = new Component()
      ancestor.type = Component.Type.LIBRARY
      ancestor.group = c.group
      ancestor.name = c.name
      ancestor.version = c.version
      ancestor.purl = c.purl
      ancestor.hashes = sha256Only(c.hashes)
      c.hashes = null
      Ancestors ancestors = new Ancestors()
      ancestors.addComponent(ancestor)
      Pedigree pedigree = c.pedigree ?: new Pedigree()
      pedigree.ancestors = ancestors
      pedigree.notes = 'Relocated and embedded into this artifact by ' +
          'maven-shade-plugin; the ancestor component describes the ' +
          'original standalone artifact.'
      c.pedigree = pedigree
    } else {
      c.hashes = sha256Only(c.hashes)
      Pedigree pedigree = c.pedigree ?: new Pedigree()
      pedigree.notes = 'Embedded verbatim into this artifact by ' +
          'maven-shade-plugin, without package relocation; the hashes ' +
          'describe the original standalone artifact.'
      c.pedigree = pedigree
    }
  } else {
    c.isExternal = Boolean.TRUE
    c.hashes = sha256Only(c.hashes)
  }
}
if (!unmatched.isEmpty()) {
  throw new MojoFailureException(
      "SBOM post-processing: shaded artifacts missing from BOM components: ${unmatched}")
}

bomFile.setText(BomGeneratorFactory.createXml(Version.VERSION_17, bom).toXmlString(), 'UTF-8')
def errors = new XmlParser().validate(bomFile, Version.VERSION_17)
if (!errors.isEmpty()) {
  throw new MojoFailureException(
      "SBOM post-processing: ${bomFile} fails CycloneDX 1.7 validation:\n" +
      errors.collect { it.message }.join('\n'))
}
log.info("SBOM upgraded to CycloneDX 1.7: ${shadedCount} shaded and " +
    "${(bom.components?.size() ?: 0) - shadedCount} external components")
