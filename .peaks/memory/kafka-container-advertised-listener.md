---
name: kafka-container-advertised-listener
description: "容器化 Kafka 的 KAFKA_ADVERTISED_LISTENERS 必须是所有客户端都可达的地址；只在容器内可达或在宿主机才可达，都会表现为「建主题超时」而不是明确的连接错误。"
metadata:
  type: lesson
---

容器化单节点 Kafka（KRaft）最常见的配置错误，**症状具有误导性**：

```
Error while executing topic command : Timed out waiting for a node assignment. Call: createTopics
WARN Connection to node 1 (/127.0.0.1:19092) could not be established.
```

看起来像 broker 没起来，实际上是 **broker 起来了、但广播了一个客户端够不着的地址**。Kafka 客户端先连 bootstrap 地址拿到元数据，再按元数据里的地址去连真正的 broker；那个地址不可达就会一直重试到超时。

## 本会话犯的错

配成了「容器内监听 9092 + 映射到宿主机 19092 + 广播 `127.0.0.1:19092`」：

- 宿主机连 `127.0.0.1:19092` ✅（端口映射存在）
- **容器内**连 `127.0.0.1:19092` ❌（容器里那个端口上什么都没有，broker 在 9092）

于是**容器内跑的 CLI 永远连不上自己广播的地址** —— 而偏偏 `kafka-topics.sh` 这些工具就在容器里。

## 可行配置：让内外端口一致

```
docker run -d --name peaks-kafka -p 19092:19092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:3.7.0
```

要点：**broker 内部监听 19092，宿主机也映射 19092，广播 `127.0.0.1:19092`** —— 这样「宿主机」与「容器内」两个视角看到的是**同一个可达地址**。宿主机上的应用连 `127.0.0.1:19092`，容器内的 CLI 也连 `127.0.0.1:19092`，两边都通。

（若需要真正的内外分离，得用双 listener：`INTERNAL://:9092` 广播 `容器名:9092`、`EXTERNAL://:19092` 广播 `127.0.0.1:19092`。单机开发用上面的同端口法更简单。）

## 另一个会咬人的坑：Git Bash 改写容器内绝对路径

在 Git Bash（MSYS）里执行：

```
docker exec peaks-kafka /opt/kafka/bin/kafka-topics.sh ...
```

会被 MSYS 的路径转换改写成 `C:/Program Files/Git/opt/kafka/bin/...`，报 `no such file or directory`。**这会让人以为镜像里没有那些脚本。**

两种解法（任选）：命令前 `export MSYS_NO_PATHCONV=1`，或把路径包进 `sh -c "..."`。

**How to apply:** 起容器化 Kafka 后，**先用容器内的 CLI 做一次完整收发验证**（建主题 → 生产 → 消费 → 删主题），再把它交给下游使用。只看 `docker ps` 显示 `Up` 是不够的 —— 本会话里它在 `Up` 状态下完全无法建主题。验证时记得先处理 MSYS 路径转换，否则你会拿到一个假的「脚本不存在」结论。
