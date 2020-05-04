# What
experiment scripts for DSI paper experiments 

# Dataset
for all experiments, the corresponding dataset should be created and placed in the place as indicated in the soruce code, i.e., **/data/ds-5g** or **/data/ds-1g**. 

# Note
S3, Ceph and Wasabi used boto3, so their code are very much the same.

# Dependencies 
All python code run with python 3.7 with numpy, etc as shown in the code
native API packages and their versions are :
* boto3 v1.9.66, 
* boxsdk v2.7.1, 
* pydrive v1.3.1, 
* google-cloud-storage v1.26.0
