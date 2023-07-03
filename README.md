# DB Connector Repository
Welcome to our DB Connector repository. This repository contains examples of how to write connectors to our platform. These connectors facilitate the interaction with various databases, simplifying the process of extracting and manipulating data.

The provided example in this repository is a Snowflake connector. This connector serves as a template that you can use to develop connectors for other databases.

## Writing Your Own Connector
When writing your own connector, you should follow the structure provided in the Snowflake class, which inherits from the base class DBConnector. Here's a step-by-step guide on how to do it:

### 1: Inherit from DBConnector
Your new connector class should inherit from the DBConnector base class. For instance:

```python
from common.connectors.db.db_connector import DBConnector

class YourDB(DBConnector):
```

### 2: Initialize Connection
In the __init__ method, establish the connection properties for your database.

### 3: Implement Query Function
The _query method is where you execute your SQL queries and return the result as a pandas DataFrame. It's crucial to handle database-specific quirks and conventions in this method.

### 4: Implement Test Function
The test method is used to test the connection to the database and should return basic information about the database such as the list of available databases.

### 5: Implement Get Schemas Function
The get_schemas method is used to fetch all the schemas from the database. It retrieves both tables and columns information.

### 6: Implement Get Queries Function
The get_queries method retrieves the history of executed queries on the database within a specified interval.

### 7: Implement Get Views Function
The get_views method retrieves the available views in the database.

## Wrap Up
Remember, the key is to follow the template provided by the Snowflake class but adapt the specific database commands to your particular database. Once you've implemented your own connector, you can integrate it into our platform.

We encourage you to share your connectors with us and contribute to the growth of this repository. Happy coding!