#vim /home/hduser/sampleinput.txt
#hadoop fs -put /home/hduser/sampleinput.txt input/
hadoop fs -rmr output
hadoop jar /home/prateek/Desktop/CorpusCalculator.jar CorpusCalculator input/sampleinput.txt output
hadoop fs -ls output
hadoop fs -cat output/part-00000
# New interface requires -r-
#hadoop fs -cat output/part-r-00000 


