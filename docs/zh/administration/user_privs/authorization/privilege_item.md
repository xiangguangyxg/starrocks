---
displayed_sidebar: docs
sidebar_position: 20
---

# 权限项

本文详细描述了 StarRocks 中可以用于赋权的权限项及其含义。您可以通过 [GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) 和 [REVOKE](../../../sql-reference/sql-statements/account-management/REVOKE.md) 操作对用户和角色进行权限的赋予和收回。

> 注意：本文介绍的权限项从 3.0 版本开始提供，升级后的权限框架、语法与旧的系统无法兼容，请以 3.0 版本的操作说明为准。升级后，除个别操作外，您在旧系统上的大部分操作权限仍然保留。具体差异请见文档最后的升级指南。

## 权限列表

### 系统级权限 (SYSTEM)

| 权限                    | 用途                                                         |
| ----------------------- | ------------------------------------------------------------ |
| NODE                    | 用于操作节点，比如添加，删除，下线节点。为确保集群安全，此权限不可直接授予给用户和角色。`cluster_admin` 角色拥有此权限。 |
| GRANT                   | 创建用户/角色，更改用户/角色，将权限授予给用户/角色。此权限不可直接授予给用户和角色。`user_admin` 角色拥有此权限。 |
| CREATE RESOURCE GROUP   | 创建资源组。                                                 |
| CREATE RESOURCE         | 创建资源给 Spark Load 和外表使用。                           |
| CREATE EXTERNAL CATALOG | 创建 External Catalog。                                      |
| PLUGIN                  | 安装、卸载一个插件。                                         |
| REPOSITORY              | 创建、删除、查看仓库。                                       |
| BLACKLIST               | 创建、删除、查看 SQL 黑名单和 BE 黑名单。                      |
| FILE                    | 创建、删除、查看文件。                                       |
| OPERATE                 | 管理副本、配置项、变量、transaction等。                      |
| CREATE GLOBAL FUNCTION  | 创建一个全局 UDF。                                           |
| CREATE STORAGE VOLUME  | 为远程存储系统创建存储卷 (Storage Volume)。                     |

### 资源组权限 (RESOURCE GROUP)

| 权限  | 用途                                                         |
| ----- | ------------------------------------------------------------ |
| ALTER | 为指定资源组 (resource group) 增加、减少分类器 (classifier)。 |
| DROP  | 删除指定资源组。                                             |
| ALL   | 拥有对该资源组的上述所有权限。                               |

### 资源权限 (RESOURCE)

| 权限  | 用途                         |
| ----- | ---------------------------- |
| USAGE | 使用该资源 (resource)。      |
| ALTER | 更改该资源。                 |
| DROP  | 删除该资源。                 |
| ALL   | 拥有对该资源的上述所有权限。 |

### 用户权限 (USER)

| 权限        | 用途                                 |
| ----------- | ------------------------------------ |
| IMPERSONATE | 允许用户 a 以用户 b 的身份执行操作。 |

### 全局用户自定义函数权限 (GLOBAL FUNCTION)

| 权限  | 用途                             |
| ----- | -------------------------------- |
| USAGE | 在查询中使用该函数。             |
| DROP  | 删除该函数。                     |
| ALL   | 拥有对该函数的上述所有所有权限。 |

### 数据目录权限 (CATALOG)

| 对象                                             | 权限            | 用途                                       |
| ------------------------------------------------ | --------------- | ------------------------------------------ |
| CATALOG <br />（内部目录，默认名称为 default_catalog） | USAGE           | 使用 internal catalog。                    |
|                                                  | CREATE DATABASE | 在 internal catalog 里创建数据库。         |
|                                                  | ALL             | 拥有对 internal catalog 的上述所有权限。   |
| CATALOG （外部目录）                             | USAGE           | 使用 external catalog。      |
|                                                  | DROP            | 删除 external catalog。                    |
|                                                  | ALL             | 拥有对该 external catalog 的上述所有权限。 |

> 说明：StarRocks 内部数据目录不能删除。

### 数据库权限 (DATABASE)

| 权限                     | 用途                                       |
| ------------------------ | ------------------------------------------ |
| ALTER                    | 设置指定数据库的属性，重命名，设定配额等。 |
| DROP                     | 删除数据库。                               |
| CREATE TABLE             | 在数据库内创建表。                         |
| CREATE VIEW              | 创建视图。                                 |
| CREATE FUNCTION          | 创建函数。                                 |
| CREATE MATERIALIZED VIEW | 创建物化视图。                             |
| ALL                      | 拥有对该数据库的上述所有权限。             |

### 表权限 (TABLE)

| 权限   | 用途                                             |
| ------ | ------------------------------------------------ |
| ALTER  | 对表进行修改，对外表元数据进行刷新。             |
| DROP   | 删除表。                                         |
| SELECT | 查询表中数据。                                   |
| INSERT | 向表中导入数据。                      |
| UPDATE | 更新表。                                         |
| EXPORT | 从 StarRocks 表中导出数据。                      |
| DELETE | 按条件删除指定表中的数据，或者清空指定表的数据。 |
| ALL    | 拥有对该表的上述所有权限。                       |

### 视图权限 (VIEW)

| 权限   | 用途                         |
| ------ | ---------------------------- |
| SELECT | 查询视图 (view) 中的数据。   |
| ALTER  | 修改一个视图的定义。         |
| DROP   | 删除一个逻辑视图。           |
| ALL    | 拥有对该视图的上述所有权限。 |

### 物化视图权限 (MATERIALIZED VIEW)

| 权限    | 用途                                                         |
| ------- | ------------------------------------------------------------ |
| SELECT  | 查询时引用该物化视图 (materialized view) 的数据，以加速查询。 |
| ALTER   | 更改物化视图。                                               |
| REFRESH | 刷新物化视图。                                               |
| DROP    | 删除物化视图。                                               |
| ALL     | 拥有对该物化视图的上述所有权限。                             |

### 库级别用户自定义函数权限 (FUNCTION)

| 权限    | 用途                                                         |
| ------- | ------------------------------------------------------------ |
| USAGE | 使用该函数。                 |
| DROP  | 删除该函数。                 |
| ALL   | 拥有对该函数的上述所有权限。 |

### 存储卷权限 (STORAGE VOLUME)

| 权限    | 用途                                                         |
| ------- | ------------------------------------------------------------ |
| ALTER | 更改存储卷的认证属性、注释或状态（enabled）。      |
| DROP  | 删除指定存储卷。                 |
| USAGE | 查看指定存储卷的信息，设置指定存储卷为默认存储卷。   |
| ALL   | 拥有对该存储卷的上述所有权限。 |

## 升级注意事项

从 2.x 版本升级至 3.0 版本时，由于权限系统的升级，有可能导致您的部分操作无法进行。具体包含：

| **操作**       | **涉及命令**                    | **升级前**                                                   | **升级后**                                                   |
| -------------- | ------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 修改表         | ALTER TABLE，CANCEL ALTER TABLE | 拥有表或表所在数据库的 `LOAD_PRIV` 权限即可对表执行 `ALTER TABLE`，`CANCEL ALTER TABlE` 操作 | 您需要拥有对应表的 ALTER 权限才可以执行该操作。              |
| 刷新外表       | REFRESH EXTERNAL TABLE          | 拥有外表的 `LOAD_PRIV` 权限即可对外表进行刷新。              | 您需要拥有对应外表的 ALTER 权限才可以对外表进行刷新。        |
| 备份与恢复     | BACKUP，RESTORE                 | 拥有数据库的 `LOAD_PRIV` 权限即可对该数据库及其下的任意表进行备份恢复。 | 管理员需要重新对用户赋权，以保证用户可以执行`BACKUP`和`RESTORE`操作。 |
| 删除后复原     | RECOVER 数据库或表            | 拥有对应数据库或表的 `ALTER_PRIV，CREATE_PRIV`，`DROP_PRIV` 权限即可对库或表进行复原。 | 您需要拥有 default_catalog 的 CREATE DATABASE 权限才可以恢复数据库；需要拥有对应数据库的 CREATE TABLE 和对应表的 DROP 权限。 |
| 创建、更改用户 | CREATE USER，ALTER USER         | 拥有数据库的 `GRANT_PRIV` 权限即可创建和更改用户。           | 您需要拥有 `user_admin` 角色才可以执行`CREATE USER`和`ALTER USER`操作。 |
| 授予和收回权限 | GRANT，REVOKE                   | 拥有各对象的 `GRANT_PRIV` 权限即可将对应对象的权限授予给其他用户或角色。 | 升级后，您仍旧可以将您在该对象上已经拥有的权限赋予给其他用户或角色。<br />在新的权限系统中：<ul><li>拥有 `user_admin` 角色的用户才可以将任意权限授予给任意用户和角色。</li><li>当您的授权语句包含`WITH GRANT OPTION`时，您可以将该语句涉及的权限授予给其他用户或角色。 </li></ul>|

在 2.x 版本中，StarRocks 的角色实现并不完全。当您将角色赋予给用户时，StarRocks 直接将角色所有的权限赋予给用户，而并不是将角色本身赋予给用户。从而在老版本中，StarRocks 的用户和角色并没有直接的从属关系。

升级之后，StarRocks 会对每个用户拥有的权限进行升级，老的角色仍旧存在，但仍旧与用户没有从属关系。如果您希望使用新的 RBAC，可以按照新的 GRANT 操作进行赋权。
