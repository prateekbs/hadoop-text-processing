# Problem Statement
Finding the 3 most probable sentences that occur in a text file. The proabability of a word is 
P(i,	w)	=	Num(i,	w)	/	N; where	Num(i	,	w)	is	the	number	of	times	that	w	exists	at	i-th	position	of	a	sentence,	N	is	the	total	number	of	sentences	with	at	least	i	words. The	probability	of	a	sentence	is	the	product	of	probability	of	all	its	individual words.	

# Methodology
The process is done in 3 Map Reduce Stages:

Mapper 1:

TokenizerMapper is the first Mapper phase. We emit position in the sentence as key the Word in that position and the entire Sentence as value.  E.g: 
    	   sampleinput.txt has the following 2 lines:
    	   Hello hadoop 
    	   Welcome to hadoop
    	   We emit the following key value pairs (key, value) in the Map Stage:
    	   (1,<{Hello,Hello hadoop}>), (2,<{hadoop,Hello hadoop}>), 
    	   (1,<{Welcome,Welcome to hadoop}>),(2,<{to,Welcome to hadoop}>),
    	   (1,<{hadoop,Welcome to hadoop}>)
    	   
 Assumptions: The \n character denotes the beginning of a new sentence. Some preprocessing is required on the input text file to make sure that this condition holds. 


Reducer 1:

  Reducer counts the number of times every word occurs at a particular position. It obtains probability information from the count and the total number of (key, value) pairs. As the value we emit a value in the format (word probability, sentence). For the example sentences given in the mapper, we would emit (1,<Hello 0.5, hello hadoop>) and so on.

    	  
Mapper 2:

  In the second Mapper stage, we emit the sentence as the key and the probability of the word as value. This ensures that we can multiply all the probability values in the reducer stage to get the  probability of occurrence of the sentence.  (1,<Hello 0.5, hello hadoop>)  that is received from the reducer would be emitted as (<hello hadoop>,0.5).

	  
Reducer 2:

In the second reducer stage, we multiply all the probability values received to get the probability of the entire sentence. We also dump all the sentences onto a Hashmap and sort by value and get the top 3 sentences with highest probability.


Mapper 3:

The third mapper stage acts as a dummy stage that passes everything that it receives from the second reducer to the third reducer. 


Reducer 3:

Every reducer would have got the top  3 sentences with highest probability. In this stage, we do the top 3 sentences across all reducers. We need to set the number of reducers to 1 as follows: job3.setNumReduceTasks(1) in the Job parameters to get 1 single text file output.



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
