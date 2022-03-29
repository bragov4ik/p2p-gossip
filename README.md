# P2P gossip application

## Description
This is an application for creating and connecting to a simple peer to peer network. Nodes in the network periodically ping all other members and receive (and display) others' pings.

## How to use
To launch the node, you need to run it with appropriate command line arguments. List of available args you can get with the use of `--help`/`-h` or from the list below.

For example, to start the first node in the network, you can use

`p2p-gossip --period 5 --port 8080`
It starts a node that will ping others each 5 seconds and listen for new connections on TCP port 8080. Its keys will be randomly generated and stored in program memory.

`p2p-gossip --period 3 --port 8081 --connect 127.0.0.1:8080`
This will start a node that will ping others each 3 seconds, accept connections on port 8081, and will try to join the network through the node at `127.0.0.1:8080` at the startup.


### Arguments description
* `--period` (optional) time between ping messages sent to other peers. Default is 5.
* `--port` (optional) port at which the node will listen to new incoming connections. Default is 8080.
* `--connect` (optional) address through which to join the network. If nothing provided, just wait for incoming connections.
* `--key` (optional) path to file with private key. If not provided, generates a new one.
* `--cert` (optional) path to file with certificate (public key). If not provided, generates a new one. 

## Network behavior and summary
This is a fully-connected network, which means all nodes directly communicate to each other. This allows to simplify ping distribution process, since each peer is responsible only for itself.

### Joining
### Authentication and (some) security
Nodes have identity that consist of their hashed public keys. When a node joins the network, peers simply add its identity to their known peers list. On the following reconnections to the network, the peers receive the same identity and match it to given public key, authenticating the node this way (in fact, the node also verifies other peers the same way).

*pic of authentication and identity comparison*

The traffic is encrypted using TLS with certificate verification procedure described above, i.e. known peers' public keys are compared with their identities and new peers are simply accepted and rememebered.
### Status check
Network uses heartbeat messages for checking status of other nodes. If a node doesn't receive a heartbeat from another for some time, it considers the peer dead. 

*pic of heartbeat*

### Connection loss
If a node losses connection to the peer (either by closing TCP connection or absence of heartbeat) it starts reconnection procedure. On reconnection, the peers authenticate each other and exchange lists of known peers. It is done for the case of network splitting, when new nodes are added to both sides.

*pic of simple reconnecition*

*picture of network split*

<!-- ## Possible further improvements
* sadsad -->
