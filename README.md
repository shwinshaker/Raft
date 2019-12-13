# Raft
Python implementation of Raft, following multi-thread event driven schema

**Usage**
* Build a distributed node locally
`./run-server.sh localhost:8080`
* Update files, get files, or crash some nodes as your wish by modifying `/src/test.py`, then
`./run-tester.sh`

**Reference**
* [Raft Consensus Algorithm](https://raft.github.io)
* [Understand Distributed Consensus](http://thesecretlivesofdata.com/raft/)
* [Raft Animation](https://raft.github.io/raftscope/index.html)
* [Raft-project](https://github.com/nathanvnbg/Raft-Project)
