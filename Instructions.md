**Apache Stack Installation Guide (WSL Ubuntu)**

**Prerequisites & Downloads**
Ensure you have downloaded these exact files to your Windows Downloads folder:

kafka_2.13-3.7.1.tgz (https://kafka.apache.org/community/downloads/#371) 
spark-3.5.1-bin-hadoop3.tgz (https://archive.apache.org/dist/spark/spark-3.5.1/) and 
project specific .jar files (included in this folder) 

**Note:** I've used Apache spark 3.5.1, Delta Lake 3.2.0, Airfkow 2.9.3, Kafka 3.7.0

**Phase 1: OS and Core Dependencies**

**1. Enable WSL** Enable or Install WSL (Windows Subsystem for Linux) on your windows machine 
      via Windows Features or Powershell
**2. Install Ubuntu 24.04** Open Terminal(Command Prompt) or Powershell as Administrator and execute command 
      wsl --install --distribution Ubuntu-24.04, 
**3. Update OS Packages** Once Installed set Username and password and run below command to update the linux packages 
      sudo apt update -y && sudo apt upgrade -y
**4. Install Java Develpoment Kit** Install jdk 17 using command sudo apt install openjdk-17-jdk -y

**Phase 2: Apche Spark & Kafka Installation**

**5. Move Archieves to WSL** Copy kafka and Spark .tgz files from Windows to wsl home directory
    cp /mnt/c/Users/<Your Name>/Downloads/kafka_2.13-3.7.1.tgz ~
    cp /mnt/c/Users/<Your Name>/Downloads/spark-3.5.1-bin-hadoop3.tgz ~
**6. Extract & Rename Folder** After copying extract to respective folders using below commands
    tar -xzf ~/kafka_2.13-3.7.1.tgz -C ~ && mv ~/kafka_2.13-3.7.1 ~/kafka
    tar -xzf ~/spark-3.5.1-bin-hadoop3.tgz -C ~ && mv ~/spark-3.5.1-bin-hadoop3 ~/spark
**7. Clean Archives** Remove the .tgz files using command rm *.tgz
**8. Load Custom jar files** copy .jar files (included in this folder) inside /home/<your_name>/spark/jars folder using command cp /mnt/c/Users/<your name>/Downloads/*.jar ~/spark/jars/
**9. Build Medallion Directory Structure** Create Directory (Medallion architecture) using command 
mkdir -p Datalake/{Bronze,Silver,Gold}

**Phase 3: Kafka Cluster Initialization**

**10.Navigate to Kafka Home** Now to enable kafka, we need to generate cluster id, so get into kafka folder 
    using command cd ~/kafka
**11. Generate Unique Cluster Id** Run the below command 
    KAFKA_CLUSTER_ID=$(~/kafka/bin/kafka-storage.sh random-uuid)
    echo "Your Cluster ID is: $KAFKA_CLUSTER_ID"
**12. Kafka clusterId** Your Cluster ID is: 5T***************bA
**13.Format and Boot Kafka Log Directories** Then run the below commands one after the other
    ~/kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c ~/kafka/config/kraft/server.properties
    ~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/kraft/server.properties

**Phase 4: Python Virtual Environment & Airflow**

**14. Download Native Linux Python tools** Open new Ubuntu terminal and run command 
sudo apt update && sudo apt install -y python3-pip python3-venv 
to install python virtual environment libraries
**15. Create Vitual Environment** Once libraries are installed, run below command to create and activate virtual environment
    python3 -m venv ~/lakehouse
    source ~/lakehouse/bin/activate
**16. Install python Dependencies** While python venv is active, need to install pyspark, delta-storage compatible for airflow 2.9.3 using below commands
    pip install pyspark==3.5.1 delta-spark==3.2.0 
    pip install "apache-airflow-providers-apache-spark==4.8.2"
    
**17. Airflow version Constraints** Before installing airflow, create a contraint file with contents in https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt which instructs pip install specific versions of required libraries
    command: nano /home/<your_name>/lakehouse/constraints.txt
    It opens a window where below content needs to be pasted, then presss Ctrl+O (Save), then Enter (confirm FileName) and Ctrl+X (Close)

**18. Download Airflow 2.9.3** Keeping python venv active, run command 
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION=3.12

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
  
**19. verify Engine State** post installion is complete run command airflow version and 
check version to be displayed as 2.9.3

**Phase 5: Database Setup & Webserver Deployment**

**20. Initialize metadata storage** Now we need to initiate the airflow by below commands one after the other
    airflow db init
    airflow db migrate
**21. Provision Admin Identity** Create the user using command:
    airflow users create \
    --username admin \
    --firstname <Your_Name> \
    --lastname <Last_Name> \
    --role Admin \
    --email <your_email> \
    --password <password>
**22. Establish Dag Drop-Zone** Open fresh Ubuntu terminal and run command mkdir -p ~/airflow/dags 
    to create folder and create/place the python script which create DAG(ETL/ELT Pipeline)
**23. Stage Custom Pipeline Script** Run command nano ~/airflow/dags/kafka_spark_delta_dag.py which opens window where the python script can be written and saved (Ctrl+O, Enter, Ctrl+X)
**24. Silence Example Pipelines** Run command nano ~/airflow/airflow.cfg which opens config file of airflow, scroll down to line where it says load_examples=True, change the value to False and save the file (Ctrl+O, Enter, Ctrl+X)
**25. Run Airflow Engines** Open Ubuntu terminal with Python venv activated and run commands one after the other
    airflow scheduler -D
    airflow webserver --port 8085 -D
**26. Access COntrol Panel** Open browser and browse http://localhost:8085/home which gives the view of airflow homepage with a DAG created
**27. Orchestrate Tasks** Locate your custom pipeline, toggle it to Active, and Click on Play button to Trigger the DAG, then click on DAG name and switch to Graph view to view the progress.




    
