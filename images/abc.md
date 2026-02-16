flowchart LR

    subgraph EC2_1["EC2 Instance 1 - Airflow"]
        AF[Airflow Scheduler + Webserver]
    end

    subgraph EC2_2["EC2 Instance 2 - Spark Cluster"]
        SM[Spark Master]
        SW1[Spark Worker]
        SW2[Spark Worker]
    end

    subgraph EC2_3["EC2 Instance 3 - Cassandra"]
        CS[Cassandra Node]
    end

    AF -->|Submit Spark Job| SM
    SM --> SW1
    SM --> SW2

    SW1 -->|Write Data| CS
    SW2 -->|Write Data| CS
