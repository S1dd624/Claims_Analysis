Project Intent:

The 'Claims_Analysis' is a systemic integration attempted between Linux process management, Hadoop Distributed File System (HDFS) components, Spark, PySpark and Python
offering a compatible pipeline between the aforementioned softwares. Insights from the deployed cluster to be upscaled.

Working Stack:

These versions were and are not interchangeable during developement.

OS: Siduction	Linux (Debian unstable distro) 
Java: OpenJDK 8	Required for Spark 3.1.2 stability.
Hadoop:	3.3.6	Single-node pseudo-distributed.
Spark:	3.1.2	
Python:	3.10.14	Managed via pyenv.
PySpark:	3.1.2	Installed via pip inside the virutal env.
pandas:	TO be compatible with Python 3.10, must include _ctypes support
seaborn / matplotlib:	Standard releases, visualization purposes

Python 3.12+ or 3.13 incompatible.
PySpark 3.1.x and pandas shall break due to a lack standard library modules.

Directory Structure:

/home/user/
├── hadoop-3.3.6/
├── spark-3.1.2/
└── <project>/
    ├── claims_analysis.py
    ├── upload_hdfs.sh
    ├── requirements.txt
    └── .venv/

    
