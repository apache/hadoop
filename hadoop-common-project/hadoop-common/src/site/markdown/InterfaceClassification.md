<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Hadoop Interface Taxonomy: Audience and Stability Classification
================================================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Motivation
----------

The interface taxonomy classification provided here is for guidance to
developers and users of interfaces. The classification guides a developer to
declare the targeted audience or users of an interface and also its stability.

* Benefits to the user of an interface: Knows which interfaces to use or not use and their stability.

* Benefits to the developer: to prevent accidental changes of interfaces and
  hence accidental impact on users or other components or system. This is
  particularly useful in large systems with many developers who may not all have
  a shared state/history of the project.

Interface Classification
------------------------

Hadoop adopts the following interface classification,
this classification was derived from the
[OpenSolaris taxonomy](http://www.opensolaris.org/os/community/arc/policies/interface-taxonomy/#Advice)
and, to some extent, from taxonomy used inside Yahoo.
Interfaces have two main attributes: Audience and Stability

### Audience

Audience denotes the potential consumers of the interface. While many interfaces
are internal/private to the implementation, other are public/external interfaces
are meant for wider consumption by applications and/or clients. For example, in
posix, libc is an external or public interface, while large parts of the kernel
are internal or private interfaces. Also, some interfaces are targeted towards
other specific subsystems.

Identifying the audience of an interface helps define the impact of breaking
it. For instance, it might be okay to break the compatibility of an interface
whose audience is a small number of specific subsystems. On the other hand, it
is probably not okay to break a protocol interfaces that millions of Internet
users depend on.

Hadoop uses the following kinds of audience in order of increasing/wider visibility:

> Hadoop doesn't have a Company-Private classification, which is meant for APIs
> which are intended to be used by other projects within the company, since it
> doesn't apply to opensource projects. Also, certain APIs are annotated as
> @VisibleForTesting (from com.google.common .annotations.VisibleForTesting) -
> these are meant to be used strictly for unit tests and should be treated as
> "Private" APIs.

#### Private

The interface is for internal use within the project (such as HDFS or MapReduce)
and should not be used by applications or by other projects. It is subject to
change at anytime without notice. Most interfaces of a project are Private (also
referred to as project-private).

#### Limited-Private

The interface is used by a specified set of projects or systems (typically
closely related projects). Other projects or systems should not use the
interface. Changes to the interface will be communicated/ negotiated with the
specified projects. For example, in the Hadoop project, some interfaces are
LimitedPrivate{HDFS, MapReduce} in that they are private to the HDFS and
MapReduce projects.

#### Public

The interface is for general use by any application.

### Stability

Stability denotes how stable an interface is, as in when incompatible changes to
the interface are allowed. Hadoop APIs have the following levels of stability.

#### Stable

Can evolve while retaining compatibility for minor release boundaries; in other
words, incompatible changes to APIs marked Stable are allowed only at major
releases (i.e. at m.0).

#### Evolving

Evolving, but incompatible changes are allowed at minor release (i.e. m .x)

#### Unstable

Incompatible changes to Unstable APIs are allowed any time. This usually makes
sense for only private interfaces.

However one may call this out for a supposedly public interface to highlight
that it should not be used as an interface; for public interfaces, labeling it
as Not-an-interface is probably more appropriate than "Unstable".

Examples of publicly visible interfaces that are unstable
(i.e. not-an-interface): GUI, CLIs whose output format will change

#### Deprecated

APIs that could potentially removed in the future and should not be used.

How are the Classifications Recorded?
-------------------------------------

How will the classification be recorded for Hadoop APIs?

* Each interface or class will have the audience and stability recorded using
  annotations in org.apache.hadoop.classification package.

* The javadoc generated by the maven target javadoc:javadoc lists only the public API.

* One can derive the audience of java classes and java interfaces by the
  audience of the package in which they are contained. Hence it is useful to
  declare the audience of each java package as public or private (along with the
  private audience variations).

FAQ
---

* Why aren’t the java scopes (private, package private and public) good enough?
    * Java’s scoping is not very complete. One is often forced to make a class
      public in order for other internal components to use it. It does not have
      friends or sub-package-private like C++.

* But I can easily access a private implementation interface if it is Java public.
  Where is the protection and control?
    * The purpose of this is not providing absolute access control. Its purpose
      is to communicate to users and developers. One can access private
      implementation functions in libc; however if they change the internal
      implementation details, your application will break and you will have
      little sympathy from the folks who are supplying libc. If you use a
      non-public interface you understand the risks.

* Why bother declaring the stability of a private interface?
  Aren’t private interfaces always unstable?
    * Private interfaces are not always unstable. In the cases where they are
      stable they capture internal properties of the system and can communicate
      these properties to its internal users and to developers of the interface.
        * e.g. In HDFS, NN-DN protocol is private but stable and can help
          implement rolling upgrades. It communicates that this interface should
          not be changed in incompatible ways even though it is private.
        * e.g. In HDFS, FSImage stability can help provide more flexible roll backs.

* What is the harm in applications using a private interface that is stable? How
  is it different than a public stable interface?
    * While a private interface marked as stable is targeted to change only at
      major releases, it may break at other times if the providers of that
      interface are willing to changes the internal users of that
      interface. Further, a public stable interface is less likely to break even
      at major releases (even though it is allowed to break compatibility)
      because the impact of the change is larger. If you use a private interface
      (regardless of its stability) you run the risk of incompatibility.

* Why bother with Limited-private? Isn’t it giving special treatment to some projects?
  That is not fair.
    * First, most interfaces should be public or private; actually let us state
      it even stronger: make it private unless you really want to expose it to
      public for general use.
    * Limited-private is for interfaces that are not intended for general
      use. They are exposed to related projects that need special hooks. Such a
      classification has a cost to both the supplier and consumer of the limited
      interface. Both will have to work together if ever there is a need to
      break the interface in the future; for example the supplier and the
      consumers will have to work together to get coordinated releases of their
      respective projects. This should not be taken lightly – if you can get
      away with private then do so; if the interface is really for general use
      for all applications then do so. But remember that making an interface
      public has huge responsibility. Sometimes Limited-private is just right.
    * A good example of a limited-private interface is BlockLocations, This is
      fairly low-level interface that we are willing to expose to MR and perhaps
      HBase. We are likely to change it down the road and at that time we will
      have get a coordinated effort with the MR team to release matching
      releases. While MR and HDFS are always released in sync today, they may
      change down the road.
    * If you have a limited-private interface with many projects listed then you
      are fooling yourself. It is practically public.
    * It might be worth declaring a special audience classification called
      Hadoop-Private for the Hadoop family.

* Lets treat all private interfaces as Hadoop-private. What is the harm in
  projects in the Hadoop family have access to private classes?
    * Do we want MR accessing class files that are implementation details inside
      HDFS. There used to be many such layer violations in the code that we have
      been cleaning up over the last few years. We don’t want such layer
      violations to creep back in by no separating between the major components
      like HDFS and MR.

* Aren't all public interfaces stable?
    * One may mark a public interface as evolving in its early days. Here one is
      promising to make an effort to make compatible changes but may need to
      break it at minor releases.
    * One example of a public interface that is unstable is where one is
      providing an implementation of a standards-body based interface that is
      still under development. For example, many companies, in an attampt to be
      first to market, have provided implementations of a new NFS protocol even
      when the protocol was not fully completed by IETF. The implementor cannot
      evolve the interface in a fashion that causes least distruption because
      the stability is controlled by the standards body. Hence it is appropriate
      to label the interface as unstable.
