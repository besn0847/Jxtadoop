Jxtadoop
========

Hadoop on JXTA

This port requires ***JRE 1.7*** at minimum since the dependency on disk tools executbales (du & df) has been removed.

Links :
   - Architecture : http://fbe-big-data.blogspot.fr/2013/03/jxtadoop-architecture-design.html

Current version 0.8.0


Version 0.8.0
------------
- Relay communications have been enabled
- Datanodes comunicate thru multicast wherever possible
- Now supporting 10 replication streams per datanode in parallel

Version 0.7.0
------------
- Installer for Windows and Linux
- Desktop tray icon with Info & Shutdown menu
- Windows port for Datanode 

Version 0.6.1
------------
- Added p2p command line to analyze peer-to-peer cloud
- Adding checksum capability with fs cmd line
- Fixed few build issues
- Clean-up properties files & old classes

Version 0.6.0
------------
Fixed most stability issues. Removed most dependencies on system executables. Improved error handling.
Tested with one public NameNode, 1 private DataNode, 1 corporate DataNode + 5 cloud DataNodes.

Version 0.5.0
------------
Initial release with some stability issues and memory.



Known issues
-----------
. The DFS to DFS copy command does not work
. Randomly the SRDI peer map gets corrupted. Just remove var/p2p/* on NN & restart.
