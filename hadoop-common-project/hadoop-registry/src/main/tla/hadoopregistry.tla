---------------------------- MODULE hadoopregistry ----------------------------

EXTENDS FiniteSets, Sequences, Naturals, TLC


(*
============================================================================
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
============================================================================
 *)

(*

============================================================================

This defines the Hadoop registry in terms of operations on sets of records.

Every registry entry is represented as a record containing both the path and the data.

It assumes that

1. operations on this set are immediate.
2. selection operations (such as \A and \E are atomic)
3. changes are immediately visible to all other users of the registry.
4. This clearly implies that changes are visible in the sequence in which they happen.

A multi-server Zookeeper-based registry may not meet all those assumptions

1. changes may take time to propagate across the ZK quorum, hence changes cannot
be considered immediate from the perspective of other registry clients.
(assumptions (1) and (3)).

2. Selection operations may not be atomic. (assumption (2)).

Operations will still happen in the order received by the elected ZK master

A stricter definition would try to state that all operations are eventually
true excluding other changes happening during a sequence of action.
This is left as an excercise for the reader.

The specification also omits all coverage of the permissions policy.
*)



CONSTANTS
    PathChars,     \* the set of valid characters in a path
    Paths,         \* the set of all possible valid paths
    Data,          \* the set of all possible sequences of bytes
    Address,       \* the set of all possible address n-tuples
    Addresses,     \* the set of all possible address instances
    Endpoints ,    \* the set of all possible endpoints
    PersistPolicies,\* the set of persistence policies
    ServiceRecords, \* all service records
    Registries,     \* the set of all possile registries
    BindActions,     \* all possible put actions
    DeleteActions,  \* all possible delete actions
    PurgeActions,   \* all possible purge actions
    MknodeActions    \* all possible mkdir actions


ASSUME PathChars \in STRING
ASSUME Paths \in STRING

(* Data in records is JSON, hence a string *)
ASSUME Data \in STRING

----------------------------------------------------------------------------------------

(* the registry*)
VARIABLE registry


(* Sequence of actions to apply to the registry *)
VARIABLE actions


----------------------------------------------------------------------------------------
(* Tuple of all variables.  *)


vars == << registry, actions >>


----------------------------------------------------------------------------------------




(* Persistence policy *)
PersistPolicySet == {
    "permanent",            \* persists until explicitly removed
    "application",          \* persists until the application finishes
    "application-attempt",  \* persists until the application attempt finishes
    "container"             \* persists until the container finishes
  }

(* Type invariants. *)
TypeInvariant ==
    /\ \A p \in PersistPolicies: p \in PersistPolicySet


----------------------------------------------------------------------------------------



(*

An Entry is defined as a path, and the actual
data which it contains.

By including the path in an entry, we avoid having to define some
function mapping Path -> entry.  Instead a registry can be defined as a
set of RegistryEntries matching the validity critera.

*)

RegistryEntry == [
    \* The path to the entry
    path: Paths,

    \* the data in the entry
    data: Data
    ]


(* Define the set of all string to string mappings *)

StringMap == [
  STRING |-> STRING
]



(*
    An endpoint in a service record
*)
Endpoint == [
    \* API of the endpoint: some identifier
    api: STRING,

    \* A list of address n-tuples
    addresses: Addresses
]

(*
    A service record
*)
ServiceRecord == [

    \* This MUST be present: if it is not then the data is not a service record
    \* This permits shortcut scan & reject of byte arrays without parsing
    type: "JSONServiceRecord",

    \*A description
    description: STRING,

    \* A set of endpoints
    external: Endpoints,

    \* Endpoints intended for use internally
    internal: Endpoints,

    \* Attributes are a function
    attributes: StringMap
]

----------------------------------------------------------------------------------------

(*
 There is an operation serialize whose internals are not defined,
 Which converts the service records to JSON
 *)

CONSTANT serialize(_)

(* A function which returns true iff the byte stream is considered a valid service record. *)
CONSTANT containsServiceRecord(_)

(* A function to deserialize a string to JSON *)
CONSTANT deserialize(_)

ASSUME \A json \in STRING: containsServiceRecord(json) \in BOOLEAN

(* Records can be serialized *)
ASSUME \A r \in ServiceRecord : serialize(r) \in STRING /\ containsServiceRecord(serialize(r))

(* All strings for which containsServiceRecord() holds can be deserialized *)
ASSUME \A json \in STRING: containsServiceRecord(json) => deserialize(json) \in ServiceRecord




----------------------------------------------------------------------------------------

(* Action Records *)

putAction == [
    type: "put",
    record: ServiceRecord
]

deleteAction == [
    type: "delete",
    path: STRING,
    recursive: BOOLEAN
]

purgeAction == [
    type: "purge",
    path: STRING,
    persistence: PersistPolicySet
]

mkNodeAction == [
    type: "mknode",
    path: STRING,
    parents: BOOLEAN
]


----------------------------------------------------------------------------------------

(*

 Path operations

*)

(*
Parent is defined for non empty sequences
 *)

parent(path) == SubSeq(path, 1, Len(path)-1)

isParent(path, c) == path = parent(c)

----------------------------------------------------------------------------------------
(*
Registry Access Operations
*)

(*
Lookup all entries in a registry with a matching path
*)

resolve(Registry, path) == \A entry \in Registry: entry.path = path

(*
A path exists in the registry iff there is an entry with that path
*)

exists(Registry, path) == resolve(Registry, path) /= {}

(*
A parent entry, or an empty set if there is none
*)
parentEntry(Registry, path) == resolve(Registry, parent(path))

(*
A root path is the empty sequence
*)
isRootPath(path) == path = <<>>

(*
The root entry is the entry whose path is the root path
*)
isRootEntry(entry) == entry.path = <<>>


(*
A path p is an ancestor of another path d if they are different, and the path d
starts with path p
*)

isAncestorOf(path, d) ==
    /\ path /= d
    /\ \E k : SubSeq(d, 0, k) = path


ancestorPathOf(path) ==
    \A a \in Paths: isAncestorOf(a, path)

(*
The set of all children of a path in the registry
*)

children(R, path) == \A c \in R: isParent(path, c.path)

(*
A path has children if the children() function does not return the empty set
*)
hasChildren(R, path) == children(R, path) /= {}

(*
Descendant: a child of a path or a descendant of a child of a path
*)

descendants(R, path) == \A e \in R: isAncestorOf(path, e.path)

(*
Ancestors: all entries in the registry whose path is an entry of the path argument
*)
ancestors(R, path) == \A e \in R: isAncestorOf(e.path, path)

(*
The set of entries that are a path and its descendants
*)
pathAndDescendants(R, path) ==
    \/ \A e \in R: isAncestorOf(path, e.path)
    \/ resolve(R, path)


(*
For validity, all entries must match the following criteria
 *)

validRegistry(R) ==
        \* there can be at most one entry for a path.
        /\ \A e \in R: Cardinality(resolve(R, e.path)) = 1

        \* There's at least one root entry
        /\ \E e \in R: isRootEntry(e)

        \* an entry must be the root entry or have a parent entry
        /\ \A e \in R: isRootEntry(e) \/ exists(R, parent(e.path))

        \* If the entry has data, it must contain a service record
        /\ \A e \in R: (e.data = << >> \/ containsServiceRecord(e.data))


----------------------------------------------------------------------------------------
(*
Registry Manipulation
*)

(*
An entry can be put into the registry iff
its parent is present or it is the root entry
*)
canBind(R, e) ==
    isRootEntry(e) \/ exists(R, parent(e.path))

(*
'bind() adds/replaces an entry if permitted
*)

bind(R, e) ==
    /\ canBind(R, e)
    /\ R' = (R \ resolve(R, e.path)) \union {e}


(*
mknode() adds a new empty entry where there was none before, iff
-the parent exists
-it meets the requirement for being "bindable"
*)

mknodeSimple(R, path) ==
    LET entry == [ path |-> path, data |-> <<>>  ]
    IN  \/ exists(R, path)
        \/ (exists(R, parent(path))  /\ canBind(R, entry) /\ (R' = R \union {entry} ))


(*
For all parents, the mknodeSimple() criteria must apply.
This could be defined recursively, though as TLA+ does not support recursion,
an alternative is required


Because this specification is declaring the final state of a operation, not
the implemental, all that is needed is to describe those parents.

It declares that the mknodeSimple() state applies to the path and all
its parents in the set R'

*)
mknodeWithParents(R, path) ==
    /\ \A p2 \in ancestors(R, path) : mknodeSimple(R, p2)
    /\ mknodeSimple(R, path)


mknode(R, path, recursive) ==
   IF recursive THEN mknodeWithParents(R, path) ELSE mknodeSimple(R, path)

(*
Deletion is set difference on any existing entries
*)

simpleDelete(R, path) ==
    /\ ~isRootPath(path)
    /\ children(R, path) = {}
    /\ R' = R \ resolve(R, path)

(*
Recursive delete: neither the path or its descendants exists in the new registry
*)

recursiveDelete(R, path) ==
       \* Root path: the new registry is the initial registry again
    /\ isRootPath(path) => R' = { [ path |-> <<>>, data |-> <<>> ] }
       \*  Any other entry: the new registry is a set with any existing
       \* entry for that path is removed, and the new entry added
    /\ ~isRootPath(path) => R' = R \ ( resolve(R, path) \union descendants(R, path))


(*
Delete operation which chooses the recursiveness policy based on an argument
*)

delete(R, path, recursive) ==
    IF recursive THEN recursiveDelete(R, path) ELSE simpleDelete(R, path)


(*
Purge ensures that all entries under a path with the matching ID and policy are not there
afterwards
*)

purge(R, path, id, persistence) ==
    /\ (persistence \in PersistPolicySet)
    /\ \A p2 \in pathAndDescendants(R, path) :
         (p2.attributes["yarn:id"] = id /\ p2.attributes["yarn:persistence"] = persistence)
         => recursiveDelete(R, p2.path)

(*
resolveEntry() resolves the record entry at a path or fails.

It relies on the fact that if the cardinality of a set is 1, then the CHOOSE operator
is guaranteed to return the single entry of that set, iff the choice predicate holds.

Using a predicate of TRUE, it always succeeds, so this function selects
the sole entry of the resolve operation.
*)

resolveEntry(R, path) ==
    LET l == resolve(R, path) IN
        /\ Cardinality(l) = 1
        /\ CHOOSE e \in l : TRUE

(*
 Resolve a record by resolving the entry and deserializing the result
 *)
resolveRecord(R, path) ==
    deserialize(resolveEntry(R, path))


(*
The specific action of putting an entry into a record includes validating the record
*)

validRecordToBind(path, record) ==
      \* The root entry must have permanent persistence
     isRootPath(path) => (
        record.attributes["yarn:persistence"] = "permanent"
        \/ record.attributes["yarn:persistence"]
        \/ record.attributes["yarn:persistence"] = {})


(*
Binding a service record involves validating it then putting it in the registry
marshalled as the data in the entry
 *)
bindRecord(R, path, record) ==
    /\ validRecordToBind(path, record)
    /\ bind(R, [path |-> path, data |-> serialize(record)])


----------------------------------------------------------------------------------------


(*
The action queue can only contain one of the sets of action types, and
by giving each a unique name, those sets are guaranteed to be disjoint
*)
 QueueInvariant ==
    /\ \A a \in actions:
        \/ (a \in BindActions /\ a.type="bind")
        \/ (a \in DeleteActions /\ a.type="delete")
        \/ (a \in PurgeActions /\ a.type="purge")
        \/ (a \in MknodeActions /\ a.type="mknode")


(*
Applying queued actions
*)

applyAction(R, a) ==
    \/ (a \in BindActions /\ bindRecord(R, a.path, a.record) )
    \/ (a \in MknodeActions /\ mknode(R, a.path, a.recursive) )
    \/ (a \in DeleteActions /\ delete(R, a.path, a.recursive) )
    \/ (a \in PurgeActions /\ purge(R, a.path, a.id, a.persistence))


(*
Apply the first action in a list and then update the actions
*)
applyFirstAction(R, a) ==
    /\ actions /= <<>>
    /\ applyAction(R, Head(a))
    /\ actions' = Tail(a)


Next == applyFirstAction(registry, actions)

(*
All submitted actions must eventually be applied.
*)


Liveness == <>( actions = <<>> )


(*
The initial state of a registry has the root entry.
*)

InitialRegistry == registry = {
  [ path |-> <<>>, data |-> <<>> ]
}


(*
The valid state of the "registry" variable is defined as
Via the validRegistry predicate
*)

ValidRegistryState == validRegistry(registry)



(*
The initial state of the system
*)
InitialState ==
    /\ InitialRegistry
    /\ ValidRegistryState
    /\ actions = <<>>


(*
The registry has an initial state, the series of state changes driven by the actions,
and the requirement that it does act on those actions.
*)
RegistrySpec ==
    /\ InitialState
    /\ [][Next]_vars
    /\ Liveness


----------------------------------------------------------------------------------------

(*
Theorem: For all operations from that initial state, the registry state is still valid
*)
THEOREM InitialState => [] ValidRegistryState

(*
Theorem: for all operations from that initial state, the type invariants hold
*)
THEOREM InitialState => [] TypeInvariant

(*
Theorem: the queue invariants hold
*)
THEOREM InitialState => [] QueueInvariant

=============================================================================
