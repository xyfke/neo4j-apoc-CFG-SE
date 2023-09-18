# Dataflow Function

This page describes the dataflow function that we have implemented. It covers both forward and backward path finding functions.

## ROS Applications

This section covers analyses for software that uses the ROS communication framework which works with subscribers, publishers, and topics.

### Parameter Description
All of the functions that will be discussed have the same parameters with the same purpose. As a result, we will define them in this section. 

The parameters are: 
* `startNode`: the starting node of the query
* `endNode`: the ending node of the query
* `startEdge`: the starting edge of the query (if not null, will replace the value of startNode) 
* `endEdge`: the ending edge of the query (if not null, will replace the value of startNode) 
* `cfgCheck`: boolean flag for whether or not to perform CFG validation

We vary the input combination of these function parameters (setting null and not null values) to determine which type of subquery we are working with. Currently we handle 3 types: prefix, suffix, and middle. The table below summarizes the combination of inputs. Any combination that is not in the table is consider invalid.
| Type of subquery  | startNode | endNode   | startEdge | endEdge | 
| :---:             | :---:     | :---:     | :---:     | :---:   |
| Prefix            | null      | __not null__  | __not null__  |  null   |
| Middle            | __not null__  | __not null__  | null      |  null   |
| Suffix            | __not null__  | null      | null  |  __not null__   |

### Forward Propagation

Path finding functions in this section uses a forward path finding technique that starts with either the startNode or the startEdge and stops when it reaches the endNode or endEdge.

#### Single Path

```
apoc.path.dataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck)
```
This function looks for a single dataflow path consisting of varWrites, parWrites, and retWrites from either the startNode to the endNode (middle), or startEdge to the endNode (prefix), or startNode to the endEdge (suffix).

#### All Path

```
apoc.path.allDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck)
```
This function looks for all of the possible dataflow paths consisting of varWrites, parWrites, and retWrites from either the startNode to the endNode (middle), or startEdge to the endNode (prefix), or startNode to the endEdge (suffix).

### Backward Propagation

Path finding functions in this section uses a backward path finding technique that starts with either the endNode or the endEdge and stops when it reaches the endNode or endEdge.

#### Single Path

```
apoc.path.backwardDataflowPath(startNode, endNode, startEdge, endEdge, cfgCheck)
```

#### All Path

```
apoc.path.allBackwardDataflowPaths(startNode, endNode, startEdge, endEdge, cfgCheck)
```

## Non-ROS Applications

This section covers software that do not use the ROS communication framework where cross component communication is done through function parameter passing `a-parWrite->b`.

### Parameter Description

The parameters are:
* `startEdge`: the the first dataflow related edge of the dataflow query (typically a parWrite or varWrite)
* `endEdge`: the last dataflow related edge of the dataflow query (typically a parWrite or varInfFunc)
* `cfgCheck`: boolean flag for whether or not to perform CFG validation

### Forward Propagation


#### Single Path

```
apoc.path.gmDataflowPath(startEdge, endEdge, cfgCheck)
```

#### All Paths

```
apoc.path.allGmDataflowPaths(startEdge, endEdge, cfgCheck)
```

### Backward Propagation

#### Single Path

```
apoc.path.backwardGmDataflowPath(startEdge, endEdge, cfgCheck)
```


#### All Paths

```
apoc.path.allBackwardGmDataflowPaths(startEdge, endEdge, cfgCheck)
```