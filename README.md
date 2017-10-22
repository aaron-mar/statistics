# Statistics Search Using Kudu and Spark

The purpose of this exercise is to use Kudu and Spark to ingest and analyze a sample SERP and SEO dataset. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine to run the exercise. 

### Prerequisites

The exercise requires a Linux machine, Maven, VirtualBox the Kudu quickstart demo VM and JDK 1.8 to run.

Download VirtualBox from here and install:

```
https://www.virtualbox.org/wiki/Downloads
```

Download and start up the Kudu quick start VM:

```bash
https://kudu.apache.org/docs/quickstart.html

curl -s https://raw.githubusercontent.com/cloudera/kudu-examples/master/demo-vm-setup/bootstrap.sh | bash
```

To log into the VM:

```bash
ssh demo@quickstart.cloudera

```
The password is demo.

Install JDK 1.8 onto the VM:

```bash
ssh demo@quickstart.cloudera
su -c "yum install java-1.8.0-openjdk"
```

Install Spark 2.2.0 onto the VM:

```bash
ssh demo@quickstart.cloudera
cd /tmp
curl https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz > spark-2.2.0-bin-hadoop2.7.tgz
tar xvfz spark-2.2.0-bin-hadoop2.7.tgz
sudo mv spark-2.2.0-bin-hadoop2.7 /opt
```

Download the CSV dataset onto the VM and into HDFS:

```bash
ssh demo@quickstart.cloudera
cd /tmp
curl https://stat-ds-test.s3.amazonaws.com/getstat_com_serp_report_201707.csv.gz > getstat_com_serp_report_201707.csv.gz
gunzip getstat_com_serp_report_201707.csv.gz
hdfs dfs -put /tmp/getstat_com_serp_report_201707.csv /tmp
hdfs dfs -chmod 777 /tmp/getstat_com_serp_report_201707.csv
```
The file goes into HDFS so that the Spark CSV reader can pick it up from there.

### Installing

After cloning this project, follow these steps to run the exercise:

```bash
cd <where the project is cloned>
scp scripts/stat.sh demo@quickstart.cloudera:/tmp
mvn clean install
scp target/stat-1.0-SNAPSHOT.jar demo@quickstart.cloudera:/tmp
```

## Running the tests

To run the script on the VM:

```bash
ssh demo@quickstart.cloudera
bash /tmp/stat.sh
```
At the point, the spark-shell will be started with the built jar. To run the exercise, type the following at the spark-shell prompt:

```scala
import org.stat.StatMain

val stat = new StatMain(sc, spark)

stat.run("/tmp/getstat_com_serp_report_201707.csv")
```
The program will let you know what it is doing and give the results.

An additional analysis was done to find the Jaccard similarity without using minhash. This analysis uses more memory than
the VM has, so to run it, exit the current spark-shell (ctrl-c) and start again:

```bash
bash /tmp/stat.sh
```
Once back into spark-shell, run the following:
```scala
import org.stat.StatMain

val stat = new StatMain(sc, spark)

stat.similarity("/tmp/getstat_com_serp_report_201707.csv")
```
## Notes

* The code is written in Scala. 
* First attempt was to import into Accumulo, however, it proved to be very slow
* Second attempt was to import into Kudu which is much faster
* Import process depended on Spark to read in the CSV file into a dataframe and export to the different data stores
* Spark seemed like a good choice to perform the analysis, although Impala could have been used with Kudu
* Using Spark allowed reading the CSV file in directly without needing Kudu since the target CSV was small enough
* The code can still be optimized, for example:
  * Create a binary key based on the tuple instead of using the tuple itself (Spark will not have to distribute as much and memory should be smaller)
  * The base seed for the minhash is questionable; could try converting the tuple to a BigInteger and hash that instead
* The similarity analysis does not seem to work as well as hoped; the difference between similarities of the actual and the estimate are more than 10%
