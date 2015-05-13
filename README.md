# hadoop-text-processing
Finding which sentences occur with highest probability in a text file using Hadoop.
# Steps on Amazon EMR
1) Create an account on AWS. 

2) Create a bucket on S3 and create 2 directories inside it: inputpath and jarpath.

3) Upload the jar exported from eclipse in the jar path and the input text file in the input path.

4) Launch a cluster on Amazon EMR. Change some settings as follows: Disable termination protection, 
Select the option to auto terminate the cluster on the completion of the job. Select the option for
custom JAR and give the path to the JAR in S3. Also, specify the input path and output path in the text box 
below the JAR path. If the bucket name is dist-soft, then the 2 paths that need to be entered are
s3://dist-soft/inputpath/ and s3://dist-soft/outputpath/. The output path should not already exist. 

5) Run the cluster and wait for a while. If the example runs successfully, then examine the outputpath in the S3 bucket. 
Otherwise, examine the logs of the Cluster and fix any issues. 
