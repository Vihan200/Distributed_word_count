# 🧠 Paxos-Inspired Distributed Dynamic Node Coordination System

A distributed text analysis platform inspired by the **Paxos consensus algorithm**, built using **Python**, **Apache Kafka**, and **Docker**. This system simulates core Paxos roles—**Coordinator**, **Proposer**, **Acceptor**, and **Learner**—to achieve leader election, consistent coordination, and real-time analysis in a fault-tolerant and scalable environment.

---

## 🚀 Features

- 📦 **Microservice Architecture** with role-based components (Coordinator, Proposer, Acceptor, Learner)
- 🔁 **Leader Election Mechanism** for dynamic coordination
- ⚙️ **Multi-threaded Execution** for parallel processing and improved throughput
- 🛡️ **Fault Tolerance** using Kafka as a resilient message broker
- 🧩 **Sidecar Proxy Integration** for modularity and enhanced communication
- 📊 **Real-Time Simulation** with structured logging and sequence diagram visualization

---

## 🧱 System Architecture

```mermaid
graph TD;
    Client -->|Requests| Coordinator
    Coordinator -->|Propose| Proposer
    Proposer -->|Send Proposal| Acceptor
    Acceptor -->|Send Acknowledgement| Proposer
    Proposer -->|Send Accepted Value| Learner
    Learner -->|Log & Respond| Client
    Kafka --> AllNodes
