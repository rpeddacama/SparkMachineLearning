outcome_data <- read.csv("NYPD_Motor_Vehicle_Collisions-2.csv", header = T, na.strings=c("",".","NA"), colClasses = "character")
  
write.csv(outcome_data, "fulldata.csv", row.names=TRUE)
