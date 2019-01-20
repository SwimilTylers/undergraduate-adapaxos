Adapaxos
-----

*last update 2018.12.21*

项目Repo: [adapaxos](https://github.com/SwimilTylers/adapaxos)

Adapaxos是对paxos自适应机制的一个探索性尝试。
初步的计划是实现paxos和disk-paxos自适应的转化。
这是一个较大的工程，因此需要将整个项目划分为多个阶段分阶段实现（防止自己跑偏）。
考虑到之前没有使用Java进行网络通信的经验，因此需要从较基础的部分开始实现。
对于Adapaxos项目，希望能够做到[*Prospect*]：

+ 代码功能实现
+ 工程化管理
+ 自动化检验工具
+ 改进代码风范

为了实现[*Prospect*]，目前将整个工程分为多个大的阶段[*Phase*]，目前进度是:
```
Macros := { 
 *  First Implementation of paxos & disk-paxos,
    Customed Configuration,
    Failure Recovery,
    Framework
}
```

该划分可能随着工作的进展进行变动。[*Phase*]的划分比较粗，可能难以控制进度。
因此需要根据当前的实验进度，进行更加细致的划分。当前进度(Macros.i)下，
第一要务是**尽快实现paxos和disk-paxos**，因此，请注意：
1）暂时不用工程框架；
2）暂时不考虑故障恢复；
3）使用静态的配置，或者**简单**从命令行中选择。

当前[*Micros*]为**paxos实现**，完成的工作为[*Echo Agenda*]，完成不分先后。
当前工作进展有

```pseudocode
Def Echo :=
 #  scmodel: "simple network establishment"
    javadoc: "sufficient comments with javadoc"
 *  rsm: "replicate state machine"
 #  agents: "proposer, acceptor & learner"
    poly: "polymorphic-lize"
.
```

希望通过细致的划分任务能够快速的实现本项目！

## 实现模型

### Proposer

1. Proposer借助ProposerNetService完成通讯
2. Proposer在整个系统中存在多个，如果发生冲突，就随机指数后退
3. Proposer时钟是全局时钟，假设不存在时间差
4. Proposer自动选择proposal number
5. Proposer每次只发出一个提案，在提案结束之前，不会进行下一次提案
6. Proposer每次的提案只有三种结束的方式
   1. 本Proposer率先达成共识：结束，准备全新提案
   2. 本次提案超时：结束，抛出ProposalTimeoutException，Learner决定:TODO
7. Proposer每次（重新）提交提案的条件是：
   1. 全新提案：立即
   2. Learner超时：随即指数后退

### Acceptor

1. Acceptor借助AcceptorNetService完成通讯
2. Acceptor在整个系统中存在多个，是共识产生的地方
3. Acceptor时钟是全局时钟，假设不存在时间差
4. Acceptor有存储的组件，但是存储的是当前的proposal
5. Acceptor每次accept一个proposal，发送通知给Learner

### Learner

1. Learner借助LearnerNetService完成通讯
2. Learner在整个系统中存在多个，每一个server只有一个
3. Learner时钟是全局时钟，假设不存在时间差
4. Learner有存储的组件，但是存储的是所有达成的共识
5. Learner决定达成共识的提案在执行序列中位置
6. Learner每次的提案有两种结束方式
   1. 超时
   2. 完成提案，达成共识

*？learner间的心跳包 *？