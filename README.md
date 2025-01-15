# Twitter-App

*High level Architecture :*
![image](https://github.com/user-attachments/assets/60270ab9-8fab-46a9-999b-c8b74c07c8b6)


Social media platforms like Twitter play a crucial role in understanding public opinions and trends in today's digital world. The massive amounts of data these platforms generate offer both challenges and opportunities for analysis in diverse areas. This project creates a system that efficiently stores and retrieves Twitter data, offering search features to pull insights from tweets. 

The end-to-end architecture of the Twitter Search Application with several stages:
1. Dataset Ingestion: This is the initial phase where the Twitter dataset is collected and fed into the system.
2. Data Processing Layer: Once ingested, the data undergoes a processing phase where it is curated and aggregated using parallel processing techniques. This step is crucial for preparing the data for storage and ensuring it is in the optimal format for querying and analysis.
3. Data Models:
- User Data Model (PostgreSQL): User-related data is structured and stored in a PostgreSQL relational database, which is suited for managing well-defined schema and relationships.
- Tweets Data Model (Couchbase): Tweets, which may contain more varied and flexible data structures, are stored in Couchbase as a document. This non-relational database allows for efficient storage and retrieval of such data.
- Caching Layer: A dual-cache approach enhances the system’s efficiency. An LRU cache with TTL is implemented to quickly access frequently searched queries, and a RedisCache holds top-level metrics for persistent data storage. This layer significantly decreases latency and improves user experience by reducing the load on the databases.
- Streamlit UI Interface: The user interface, built with Streamlit, provides a seamless and interactive experience. It connects users to the search application, allowing them to perform complex queries.
