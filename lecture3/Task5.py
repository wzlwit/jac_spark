Task 5
Objective: How to pass configuration parameters to spark job

Use script provided under task5 directory 

1: Provide configuration parameter through config file
Prepare conf.py file with following content
# TODO:
spark.driver.memory     1G

Execute the script as following:
# TODO: 
spark-submit --properties-file conf.py python_code.py

2: Provide configuration parameter through config file along with command line config 
# TODO:
spark-submit --properties-file conf.py --conf spark.driver.memory=25G python_code.py

3: Provide configuration parameter through config file, command line config and also command line option
# TODO:
spark-submit --properties-file conf.py --conf spark.driver.memory=20G --driver-memory 30G python_code.py

Ref:
https://spark.apache.org/docs/latest/configuration.html
