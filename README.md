# P2P gossip application

## Description
This is an application for creating and connecting to a simple peer to peer network. Nodes in the network periodically ping all other members and receive (and display) others' pings.

## Network behavior and summary
This is a fully-connected network, which means all nodes directly communicate to each other. This allows to simplify ping distribution process, since each peer is responsible only for itself.

### Joining
### Authentication and (some) security
Nodes have identity that consist of their hashed public keys. When a node joins the network, peers simply add its identity to their known peers list. On the following reconnections to the network, the peers receive the same identity and match it to given public key, authenticating the node this way (in fact, the node also verifies other peers the same way).
### Status check
### Connection loss