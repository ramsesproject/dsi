# What
Experiment scripts for the paper entitled: [Design and Evaluation of a Simple Data Interface for Efficient Data Transfer Across Diverse Storage](http://arxiv.org/abs/2009.03190) 

# Dataset
for all experiments, the corresponding dataset should be created and placed in the place as indicated in the soruce code, i.e., **/data/ds-5g** or **/data/ds-1g**. 

# Note
S3, Ceph and Wasabi used boto3, so their code are very much the same.

# Dependencies 
## software packages
All python code run with python 3.7 on the server (e.g., data transfer node) with numpy, etc as shown in the code
native API packages and their versions are :
* boto3 v1.9.66, 
* boxsdk v2.7.1, 
* pydrive v1.3.1, 
* google-cloud-storage v1.26.0

The jupyter notebook can be run anywhere (need WAS connection), the SDK we used to initiate transfer is globus-sdk 1.8.0
please refer to https://globus-sdk-python.readthedocs.io/ for tutoal to create your applicationa and obtain your credential for remote code access and control.

## storage service for experiment
* We applied a free trail account on [wasabi](https://wasabi.com/) for our wasabi experiments.
* For box, we create an account specifically for this experiments, free version doesnot work becasue it does not allow creating applications. we used their monthly paid plan 
* for S3 and Google Cloud Storage, we used regulare account and paid regulaerly, i.e., no difference with a regular user.
* for Google Drive, we paid the $2.99/month plan just for bigger space to facilitat the experiments, a free version should work but need to clear the space carefully.
* as mentioned in the paper, we setup up our own Ceph system using two storage nodes on NSF Chameleon Cloud. 

## Globus support
In order to test our Globus implementation, we setup our own DTN on NSF Chameleon Cloud and installed all needed connector. In order to run the code in jupyter notebook, you need:
* create a free account on globus.org
* follow their instruactions to install and setup connector, POSIX connector is a must in ay case.
* a subscription is needed to run cloud storage connector, you may either subscribe one or contact Globus to add your DTN UUID to a testing pool
* for S3 and google cloud storage, we also evalauted the case where connector runs near the storage bucket, for that you need repeat the setup on the cloud again.

