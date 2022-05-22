# NCBI_Nuccore

Github repo: https://github.com/Rakesh9833/NCBI_Nuccore.git

Data source : https://www.ncbi.nlm.nih.gov/nuccore/ON167538

Prefect UI : https://orion-docs.prefect.io/

Code to run the Prefect:
1. pip install prefect
2. prefect backend cloud
3. prefect auth login --key pcu_du1Kczy0eJeC9AeBoVpC1W18Ss0K951E14Ia


Output: 
-Nuccore Id and data stored in a json file (Extraction done from the given dataset URL through xml file).
-Prefect UI showing the flow and task status in details. 
-Data in Mysql
-Data in Elastic search.
