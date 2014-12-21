There're multiple files in the submission, and here is the details.
Matlab files folder		-	Contains two matlab files used to generate the input files.
	ParameterSetting.m	-	Generates the input parameters file.
	TurnFormat.m		-	Turns the original data file into the input format required by the program.
src folder 			-	Contains all java codes wrote for the program.
Input.csv			-	The input data file used in the program.
OriginalData.csv		-	The original data file, data format not usable for the program.
Parameters.csv			-	The input parameters file used in the program.
ParametersOptimizationMR.jar	-	The runnable jar file of the whole project.
					There're two versions for local test and AWS test respectively.
Project Report.pdf		-	Project report.
Reference.pdf			-	The reference paper mainly used in the project.

The program requires mutiple jars from both Hadoop and other sources, so the src probably cannot be compiled and run directly.

There're four inputs needed for the program: input data file, parameter file, intermediate path, output file.
Two examples of running the jar file is as follows.

For local test (standalone mode or pseudo-distributed mode):
The jar in the submission contains all necessary classes and can be run directly with hadoop installed.
${HADOOP_DIR}/bin/hadoop jar ${PROJECT_DIR}/ParametersOptimizationMR.jar ${PROJECT_DIR}/Input.csv ${PROJECT_DIR}/Parameters.csv ${PROJECT_DIR}/Temp ${PROJECT_DIR}/Output
The DIR should be on local filesystem and HDFS respectively for standalone mode and pseudo-distributed mode.

For Amazon AWS test:
1.Open the following link
https://577740047280.signin.aws.amazon.com/console
2.Login in
Username: test
Password: test
3.Choose EMR
4.Choose the cluster on the top: Test and clone it
5.The program should begin to run
