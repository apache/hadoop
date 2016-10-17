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


# Notation

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

A formal notation such as [The Z Notation](http://www.open-std.org/jtc1/sc22/open/n3187.pdf)
would be the strictest way to define Hadoop FileSystem behavior, and could even
be used to prove some axioms.

However, it has a number of practical flaws:

1. Such notations are not as widely used as they should be, so the broader software
development community is not going to have practical experience of it.

1. It's very hard to work with without dropping into tools such as LaTeX *and* add-on libraries.

1. Such notations are difficult to understand, even for experts.

Given that the target audience of this specification is FileSystem developers,
formal notations are not appropriate. Instead, broad comprehensibility, ease of maintenance, and
ease of deriving tests take priority over mathematically-pure formal notation.

### Mathematics Symbols in this document

This document does use a subset of [the notation in the Z syntax](http://staff.washington.edu/jon/z/glossary.html),
but in an ASCII form and the use of Python list notation for manipulating lists and sets.

* `iff` : `iff` If and only if
* `⇒` : `implies`
* `→` : `-->` total function
* `↛` : `->` partial function


* `∩` : `^`: Set Intersection
* `∪` : `+`: Set Union
* `\` : `-`: Set Difference

* `∃` : `exists` Exists predicate
* `∀` : `forall`: For all predicate
* `=` : `==` Equals operator
* `≠` : `!=` operator. In Java `z ≠ y` is written as `!( z.equals(y))` for all non-simple datatypes
* `≡` : `equivalent-to` equivalence operator. This is stricter than equals.
* `∅` : `{}` Empty Set. `∅ ≡ {}`
* `≈` : `approximately-equal-to` operator
* `¬` : `not` Not operator. In Java, `!`
* `∄` : `does-not-exist`: Does not exist predicate. Equivalent to `not exists`
* `∧` : `and` : local and operator. In Java , `&&`
* `∨` : `or` : local and operator. In Java, `||`
* `∈` : `in` : element of
* `∉` : `not in` : not an element of
* `⊆` : `subset-or-equal-to` the subset or equality condition
* `⊂` : `subset-of` the proper subset condition
* `| p |` : `len(p)` the size of a variable

* `:=` : `=` :

* `` : `#` :  Python-style comments

* `happens-before` : `happens-before` : Lamport's ordering relationship as defined in
[Time, Clocks and the Ordering of Events in a Distributed System](http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf)

#### Sets,  Lists, Maps, and Strings

The [python data structures](http://docs.python.org/2/tutorial/datastructures.html)
are used as the basis for this syntax as it is both plain ASCII and well-known.

##### Lists

* A list *L* is an ordered sequence of elements `[e1, e2, ... en]`
* The size of a list `len(L)` is the number of elements in a list.
* Items can be addressed by a 0-based index  `e1 == L[0]`
* Python slicing operators can address subsets of a list `L[0:3] == [e1,e2]`, `L[:-1] == en`
* Lists can be concatenated `L' = L + [ e3 ]`
* Lists can have entries removed `L' = L - [ e2, e1 ]`. This is different from Python's
`del` operation, which operates on the list in place.
* The membership predicate `in` returns true iff an element is a member of a List: `e2 in L`
* List comprehensions can create new lists: `L' = [ x for x in l where x < 5]`
* for a list `L`, `len(L)` returns the number of elements.


##### Sets

Sets are an extension of the List notation, adding the restrictions that there can
be no duplicate entries in the set, and there is no defined order.

* A set is an unordered collection of items surrounded by `{` and `}` braces.
* When declaring one, the python constructor `{}` is used. This is different from Python, which uses the function `set([list])`. Here the assumption
is that the difference between a set and a dictionary can be determined from the contents.
* The empty set `{}` has no elements.
* All the usual set concepts apply.
* The membership predicate is `in`.
* Set comprehension uses the Python list comprehension.
`S' = {s for s in S where len(s)==2}`
* for a set *s*, `len(s)` returns the number of elements.
* The `-` operator returns a new set excluding all items listed in the righthand set of the operator.



##### Maps

Maps resemble Python dictionaries; {"key":value, "key2",value2}

* `keys(Map)` represents the set of keys in a map.
* `k in Map` holds iff `k in keys(Map)`
* The empty map is written `{:}`
* The `-` operator returns a new map which excludes the entry with the key specified.
* `len(Map)` returns the number of entries in the map.

##### Strings

Strings are lists of characters represented in double quotes. e.g. `"abc"`

    "abc" == ['a','b','c']

#### State Immutability

All system state declarations are immutable.

The suffix "'" (single quote) is used as the convention to indicate the state of the system after an operation:

    L' = L + ['d','e']


#### Function Specifications

A function is defined as a set of preconditions and a set of postconditions,
where the postconditions define the new state of the system and the return value from the function.


### Exceptions

In classic specification languages, the preconditions define the predicates that MUST be
satisfied else some failure condition is raised.

For Hadoop, we need to be able to specify what failure condition results if a specification is not
met (usually what exception is to be raised).

The notation `raise <exception-name>` is used to indicate that an exception is to be raised.

It can be used in the if-then-else sequence to define an action if a precondition is not met.

Example:

    if not exists(FS, Path) : raise IOException

If implementations may raise any one of a set of exceptions, this is denoted by
providing a set of exceptions:

    if not exists(FS, Path) : raise {FileNotFoundException, IOException}

If a set of exceptions is provided, the earlier elements
of the set are preferred to the later entries, on the basis that they aid diagnosis of problems.

We also need to distinguish predicates that MUST be satisfied, along with those that SHOULD be met.
For this reason a function specification MAY include a section in the preconditions marked 'Should:'
All predicates declared in this section SHOULD be met, and if there is an entry in that section
which specifies a stricter outcome, it SHOULD BE preferred. Here is an example of a should-precondition:

Should:

    if not exists(FS, Path) : raise FileNotFoundException


### Conditions

There are further conditions used in precondition and postcondition declarations.


#### `supported(instance, method)`


This condition declares that a subclass implements the named method
 -some subclasses of the verious FileSystem classes do not, and instead
 raise `UnsupportedOperation`

As an example, one precondition of `FSDataInputStream.seek`
is that the implementation must support `Seekable.seek` :

    supported(FDIS, Seekable.seek) else raise UnsupportedOperation
