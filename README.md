# Spark App Template

This is a template to build a Spark Application which includes:
* Separation between:
    * Pipeline
        * Ingestion
        * Processing
        * Store
    * Sourcing Data
        * From Hive
        * From HDFS
        * From Kafka 
        * From PostgreSQL
    * Serving Data
        * To PostgreSQL
        * To Hive
        * To HDFS
        * To Kafka
    * Logging and Error Handling
        * Custom Logger per Class
        * Error Handling
    * Configuration
        * Reading from property file
    * Testing
        * Unit Testing
        * Data Validation
    * Deployment
        * Start Up Pipeline