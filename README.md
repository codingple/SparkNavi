# SparkNavi  
#### A GPX trajectory combination algorithm (Spark)  
  
### Description  
This program is an implementation of the following paper:
> A Big-Data Trajectory Combination Method for Navigations using Collected Trajectory Data  
> [https://doi.org/10.9717/kmms.2016.19.2.386](https://doi.org/10.9717/kmms.2016.19.2.386)  
  
In short, this algorithm combines big trajectory data, which is GPX(GPS Xml format) file,
and generates new routes like below:  
&nbsp;  
<p align="center">
  <img src="./images/combination.png" width="70%" height="70%">
</p>  
&nbsp;  

### Prerequisite  
- Hadoop for using HDFS  
- Spark (ver. 1.2.1)
- Scala (ver. 2.10.4)  
- Java (ver. 1.7.0_75)  
  
### Overview  
The functions of each object can be defined as:  
&nbsp;  
<p align="center">
  <img src="./images/object2.png" width="80%" height="80%">
</p>  
&nbsp;  

and these objects invoke each other:  
&nbsp;  
<p align="center">
  <img src="./images/object1.png" width="80%" height="80%">
</p>  
&nbsp;  
