# Pinterest Data Pipeline

### Overview

This project aims to develop a scalable and efficient data processing system within the AWS cloud infrastructure, inspired by the data handling capabilities of major social platforms. The is to build a system that can manage large-scale data workflows, from ingestion and storage to processing and analysis.

Utilizing AWS's powerful suite of services, the system will leverage the capabilities of EC2 and Kafka for real-time data streaming and message queuing. With S3, we provide durable and secure storage for the vast amounts of data we anticipate handling. The project also involves configuring API Gateway for custom API endpoints, essential for managing data flow effectively.

Further processing and analytical operations will be conducted in Databricks, utilizing the computational power of Spark for data cleaning and complex computations. To manage and orchestrate these operations, we will utilize AWS Managed Workflows for Apache Airflow (MWAA), ensuring smooth, automated workflows.

In addition to batch processing, the system is designed to accommodate stream processing with AWS Kinesis, allowing for real-time data analytics and immediate insight generation. This dual capability ensures that our system is versatile and capable of providing timely analysis, which is critical for data-driven decision-making.

### Project Structure

<pre>
<b>pinterest-data-pipeline/</b>
├─ <b>data_emulation.py</b>
│  Code that generates
├─ <b>README.md</b>
├─ <b>LICENSE</b>
</pre>

### Cloud Architecture

<div align="center">
  <img src="arch.png" width="1000"/>
</div>