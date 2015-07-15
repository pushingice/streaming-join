# Streaming Join

The goal of the project is to benchmark techniques for querying streaming joins.
Suppose we have an event stream containing two message types `A` and `B`, such
that `A` contains foreign keys to `B`. We would like to respond to queries of
the form:
```
SELECT * FROM A
JOIN B ON A.fk_b == B.id;
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
JOIN B on A.fk_b == B.id
JOIN E on B.fk_e == E.id;
```
We could build out a set of nested queries supporting the entire tree. Further,
we could expose these to users as a REST tree:
```
/As/
/A/[a_id]
/A/[a_id]/Bs
/A/[a_id]/B/[b_id]
/A/[a_id]/B/[b_id]/Ds
/A/[a_id]/B/[b_id]/D/[d_id]
/A/[a_id]/B/[b_id]/Es
/A/[a_id]/B/[b_id]/E/[e_id]
/A/[a_id]/Cs
/A/[a_id]/C/[c_id]
```

## Data Model File Format

In `src/main/java/resources` there are CSV formatted files describing different
data models. Select a file for the run by changing the `scenarioFile` parameter
in `config.properties`. The general format is:
```
rootNode: String, childNode: String, rootWeight: int, childWeight: int
parentNode: String, childNode: String, parentWeight: int, childWeight: int
...
parentNode: String, childNode: String, parentWeight: int, childWeight: int
```
Here root node is the top of the query tree. If the data model is circular or
symmetric it does not matter which node is designated as the root. The weights
 are expressed as a ratio of data set sizes. For example `A,B,1,2` means for
 every `A` there are exactly two `B`'s.

## Usage

Do a `mvn install` to build the jar. To load messages into a running kafka
server, edit `config.properties` to point to the kafka server. Then run with
```
java -jar target/sj-driver-0.0.2-SNAPSHOT.jar config.properties
```

## Other Properties

- `messageBytes` The max size of the message payload. For each message this is
random from 0 to the max size. The content is a random string of bytes that is
base 64 encoded.
- `messageCount` Stop sending messages after this has been reached. The actual
number of messages sent depends on the scenario file.
- `randomSeed` Set the random number generator seed. This is used for repeatable
results. Can be set to any long.
- `deleteRatio` Double between 0 and 1 representing the proportion of messages
that will have a subsequent delete message sent. This feature is not implemented
yet.
- `queryDepth` This is the maximum depth of the rest tree to generate query
results for. In the example above, setting this to 3 would produce those
queries. Setting it to 2 or lower would create a restricted set.
- `prettyJson` Send and log pretty printed Json. Useful for debugging.
- `messageTopic` Topic to send content messages to.
- `queryTopic` Topic to send query responses to.




