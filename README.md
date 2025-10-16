# CRM 数据问答代理：基于火山引擎 EMR Serverless 的 NL2SQL 样例

这是一个数据分析代理项目，它允许用户通过自然语言与存储在火山引擎服务中的 CRM 数据进行问答交互。

该项目利用了先进的检索增强生成（RAG）和自然语言到 SQL（NL2SQL）技术，其核心由以下部分构成：
- **数据存储**：使用火山引擎对象存储（TOS）作为数据湖，存储 Parquet 格式的数据文件。
- **查询引擎**：利用 **火山引擎 EMR Serverless Presto** 直接对存储在 TOS 中的数据进行按量计费Serverless的 SQL 查询。
- **数据目录**：通过湖仓分析服务（LAS）的 Catalog 功能管理数据元信息。
- **AI 代理**：基于 VeADK框架构建，负责理解用户问题、生成 SQL、分析查询结果并生成最终答案。

---

## 架构概览

<p align="center">
  <img src="tutorial/img/data_agent_design.jpg" alt="架构设计图" style="width:800px;"/>
</p>

项目的工作流程如下：
1.  用户通过 Web 界面提出问题（例如：“我们在美国最重要的 5 个客户是谁？”）。
2.  Agent服务接收问题，并借助模型将其转换为针对 EMR Serverless Presto 的 SQL 查询语句。
3.  代理通过 EMR Serverless Presto 执行该 SQL 查询。
4.  EMR Serverless Presto 直接从 TOS 中读取相关的 Parquet 文件，执行计算，并将结果返回给代理。
5.  代理将查询结果和原始问题一同发送给 Gemini 模型，进行分析、总结，并生成最终的自然语言答案和可视化图表。
6.  Web 界面将答案呈现给用户。

---

## 部署与运行指南

请遵循以下步骤来配置和运行本项目。

### 1. 配置环境变量

首先，复制 `src/.env-template` 文件，创建一个名为 `.env` 的新文件，并填入您的火山引擎凭证和配置信息。

```bash
cp src/.env-template src/.env
```

然后，编辑 `src/.env` 文件，填写以下必需的配置项：

-   `VOLCENGINE_AK`: **[必需]** 您的火山引擎 Access Key ID。
-   `VOLCENGINE_SK`: **[必需]** 您的火山引擎 Secret Access Key。
-   `VOLCENGINE_REGION`: **[必需]** 您的火山引擎区域，例如 `cn-beijing`。
-   `ARK_API_KEY`: **[必需]** 您用于访问火山引擎方舟（Ark）大语言模型的 API Key。
-   `VE_LLM_MODEL_ID`: **[必需]** 您希望使用的方舟大语言模型的 Endpoint ID，例如 `doubao-pro-32k`。
-   `VE_TOS_BUCKET`: **[必需]** 您用于存储 CRM 数据的火山引擎 TOS 存储桶名称。
-   `EMR_CATALOG`: **[必需]** 您在 LAS 中为 EMR Serverless Presto 配置的 Catalog 名称。
-   `EMR_DATABASE`: **[必需]** 在上述 Catalog 下，用于存放 CRM 数据表的数据库名称。

### 2. 手动配置 LAS Catalog 和 Database

**重要提示**：当前火山引擎的 Python SDK 尚不支持通过代码直接创建 LAS 的 Catalog 和 Database。因此，您需要手动在火山引擎控制台完成以下配置。

1.  登录到火山引擎控制台。
2.  进入 **湖仓分析服务（LAS）**。
3.  在 **数据目录** -> **Catalog 管理** 中，创建一个新的 Catalog。请确保其名称与您在 `.env` 文件中配置的 `EMR_CATALOG` 值完全一致。
4.  在您刚刚创建的 Catalog 中，进入 **Database 管理**，创建一个新的 Database。请确保其名称与您在 `.env` 文件中配置的 `EMR_DATABASE` 值完全一致。

### 3. 部署演示数据与创建数据表

我们提供了一个部署脚本，它可以帮助您完成演示数据的上传和数据表的创建。

进入 `src/agents/data_agent/deploy_demo_data` 目录，并执行以下命令：

```bash
cd src/agents/data_agent/deploy_demo_data
python3 deploy_to_volcengine.py
```

该脚本会自动执行以下操作：
-   将 `sample-data` 目录下的示例 Parquet 数据文件上传到您在 `.env` 文件中配置的 TOS 存储桶。
-   读取 `all_sql_operations.sql` 文件中的 `CREATE TABLE` 语句。
-   通过 **EMR Serverless Presto** 在您配置的 LAS Catalog 和 Database 中执行这些 SQL 语句，创建外部表，使其指向您刚刚上传到 TOS 的数据。

### 4. 启动代理服务进行测试

一切准备就绪后，回到项目根目录下的 `src/agents` 目录，使用 `veadk` 命令启动 Web 服务。

```bash
cd src/agents
veadk web
```

服务启动后，在您的浏览器中打开 `http://localhost:8080`。现在，您可以开始通过自然语言与您的 CRM 数据进行对话了。

---

## 许可证

本项目根据 Apache 2.0 许可证授权 - 详情请参阅 [LICENSE](LICENSE) 文件。

## 免责声明

这不是一个官方支持的 Google 产品。本项目不符合 [Google 开源软件漏洞奖励计划](https://bughunters.google.com/open-source-security) 的资格。本仓库中的代码和数据仅用于演示目的，不适用于生产环境。
