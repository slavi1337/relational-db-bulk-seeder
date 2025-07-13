A powerful Java application designed to dynamically populate relational databases with large volumes of test data. The core of this project is its intelligent engine that analyzes database schemas to enable massive parallel data insertion, significantly speeding up the process of setting up comprehensive test environments.

It connects to a target database, discovers all tables and their foreign key relationships, and then builds a dependency graph. Using a topological sort, it determines the correct insertion order, ensuring that parent tables are always populated before their children. Tables that do not depend on each other are grouped into levels and processed concurrently by a thread pool.

For efficient and reliable database communication, the application leverages the high-performance HikariCP connection pool, ensuring that connections are managed robustly even under the load of parallel operations.

## Features

-   **Automatic Schema Discovery**: Connects to a database and automatically discovers all tables and foreign key relationships without any manual configuration.
-   **Intelligent Parallel Population**: Analyzes table dependencies using a topological sort to create an optimal execution plan. It then populates independent tables concurrently using a multi-threaded approach.
-   **Efficient Connection Management**: Utilizes the *HikariCP* connection pool for robust, high-performance database connections, essential for parallel processing.
-   **Large-Scale Data Generation**: Capable of generating over 10,000 random, type-appropriate rows for each table.
-   **Multi-Database Support**: Natively supports both **MySQL** and **PostgreSQL**.
-   **Customizable Data Generation**: Generates appropriate random data for various SQL types, including `VARCHAR`, `INTEGER`, `DECIMAL`, `BOOLEAN`, `DATE`, and `TIMESTAMP`

## How to run

Follow these instructions to build and run the project on your local machine.

### Prerequisites

-   Java Development Kit (JDK) - Version 11 or newer.
-   Apache Maven - For building the project.
-   An existing MySQL or PostgreSQL database schema.

### Instructions

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/slavi1337/relational-db-bulk-seeder.git
    cd relational-db-bulk-seeder
    ```
2.  **Configure Connection**
    Open the `src/main/java/prs/Main.java` file and modify the database connection details (host, port, database name, user, and password) inside the `switch` block to match your environment.

3.  **Build the Project**
    Open a terminal in the project's root directory and run the following Maven command:
    ```bash
    mvn clean package
    ```
    This will compile the code and create a runnable JAR file in the `target/` directory.

4.  **Run the Application**
    Execute the application using the `java -jar` command:
    ```bash
    java -jar target/relational-db-bulk-seeder-1.0-SNAPSHOT.jar
    ```
    *(Note: Replace `relational-db-bulk-seeder-1.0-SNAPSHOT.jar` with the actual name of the JAR file in your `target` directory.)*

    The application will then prompt you to select the database configuration you want to use.

## Performance Analysis

![perf](https://github.com/user-attachments/assets/ed67fc6d-001e-456e-9d46-943d1feac055)
