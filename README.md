# SparkMachineLearning

This is an implementation of Decisiontree model using Spark MLLib libraries.

It uses NewYork city motor vehicle collisions data from public website:

https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95

Source data is cleansed with R script to remove missing values.

Feature vectors used for building the model are:

Time of collision
Street name where collision happened
Borough name where collision happened

Input data is split for training and testing the decisiontree model generated.
