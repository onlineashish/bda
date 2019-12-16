#a
df <- airquality 
dim(df) 

#b
sapply(df,class) 

#c
#Printing the missing values 
print("The Missing values are as follows") 
xcolNames <- colnames(df) 
x<- colSums(is.na(df)) 
print(x) 

#d
which(is.na(df)) 
sum(is.na(df)) 
df1<- as.data.frame(df) 

#b
#Recoding the missing values 
for(i in 1:4)  
  df1[,i]<- ifelse ( is.na(df[,i]), mean(df[,i], na.rm = TRUE), df[,i]) # Excluding the missing values 
df2<-na.omit(df)

print(df2)
sum(is.na(df2))
