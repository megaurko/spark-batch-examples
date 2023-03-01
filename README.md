## How to run project locally

1. unzip the database dump file at `docker/lahman2016.zip`

2. build docker MySQL container with test data
    ```shell
    cd docker
    docker build -t mysql-test .
    ```
3. run detached database container
    ```shell
    docker run -d --name mysql-test -e MYSQL_ROOT_PASSWORD=password -p 3306:3306 mysql-test
    ```

4. compile project

    from root directory:
    ```shell
    sbt assembly
    ```
    jar file will be located at target/scala-2.13/big-data-engineer-hw-assembly-0.1.0-SNAPSHOT.jar

5. run spark submit

    copy jar to desired location and run spark-submit:
    ```shell
    spark-submit big-data-engineer-hw-assembly-0.1.0-SNAPSHOT.jar
    ```
   after running `output` folder is created and the results will appear there