Jxtadoop
========

Hadoop on JXTA

This port requires ***JRE 1.7*** at minimum since the dependency on disk tools executbales (du & df) has been removed.

Links :
   - Architecture : http://fbe-big-data.blogspot.fr/2013/03/jxtadoop-architecture-design.html

Version 0.5.0
------------
Initial release with some stability issues and memory.

Version 0.6.0
------------
Fixed most stability issues. Removed most dependencies on system executables. Improved error handling.
Tested with one public NameNode, 1 private DataNode, 1 corporate DataNode + 5 cloud DataNodes.

Known issues
-----------
. The DFS to DFS copy command does not work
. Randomly the SRDI peer map gets corrupted. Just remove var/p2p/* on NN & restart.
