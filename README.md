## HIVE Demo版
- 该项目通过实现hive的主要逻辑，旨在帮助读者对于SQL引擎有一个代码级别的基础理解。

### 项目模块的大致划分
- 下图所示，既是一条sql->物理计划的全生命流程，也是项目模块划分的基本依据。
- TODO: 
./resources/architecture.jpg


### 项目实现说明
- 对于SQL的词法解析器，这里使用与hive一致的antlr3
- 对于元数据库的支持，我们当前仅仅支持mysql作为其数据源
- 对于数据类型的支持，我们这里仅仅支持String/Int/Long，其他类型可自由扩展
- 对于操作符的支持，我们这里仅仅支持Agg/Join，暂不支持Window,不支持EXCEPT等操作
- 对于数据的获取与保存，基于HDFS来完成
- 对于计算引擎的，我们同样是基于YARN来完成计算的
- 对于SQL的支持，我们当前不支持SQL的关联查询，支持子句的查询
- 不支持支持分区
- 暂不支持复杂的数据类型(Map/Array/)


### TODO
- 后续可以支持Except等setOperator操作(从语法角度上讲比较复杂)


### 本项目不支持用于商业用途