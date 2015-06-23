# Streaming Join

The goal of the project is to benchmark techniques for querying streaming joins.
Suppose we have an event stream containing two message types `A` and `B`, such
that `A` contains foreign keys to `B`. We would like to respond to queries of
the form:
```
SELECT * FROM A
JOIN B ON A.id == B.id;
```
given the current state of the stream. This is the most basic case. We can
imagine more complex joins involving arbitrary relationships, such as trees and
graphs. For instance, a tree of relationships like this:
 ```
     A
    / \
   B   C
  / \
 D   E
 ```
would need to support queries like this, for example:
```
SELECT * FROM A
JOIN B on A.id == B.id
JOIN E on B.id == E.id;
```
We could build out a set of nested queries supporting the entire tree. Further,
we could expose these to users as a REST tree:
```
/As/
/A/[id]
/A/[id]/Bs
/A/[id]/B/[id]
/A/[id]/B/[id]/Ds
/A/[id]/B/[id]/D/[id]
/A/[id]/B/[id]/Es
/A/[id]/B/[id]/E/[id]
/A/[id]/Cs
/A/[id]/C/[id]
```
