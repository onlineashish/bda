#a
data() 
head(mtcars) 

#b
rownum <- nrow(mtcars) # Number of rows (observations) 
colnum <- ncol(mtcars) # Number of columns (variables) 

#c 
x<- data.frame(mtcars)
automatic <-0      
manual <-0 

for (i in 1:rownum)   
  ifelse( x[i,9] == 1, automatic <- automatic + 1, manual <- manual +1) 
ifelse (automatic > manual,   
        print("There are more automatic transmission type"),   
        print("There are more manual transmission type") ) 

#d
#The scatter plot 
HorsePower <- x[,4]
Weight <- x[,6]
scatter.smooth(HorsePower,Weight, span=2/3, degree = 1, family =c("symmetric","gaussian"))    

# Plot histogram of Miles/gallon 
Mpg <- x[,1]  
hist(Mpg, breaks = 12, col ="lightblue", border = "pink")     

#e.   
# Solution for e   
x[,2]<- as.integer(x[,2])  
x[,8]<- as.integer(x[,8])  
x[,9]<- as.integer(x[,9])  
x[,2] <= 5   

#f
mtcars[mtcars$cyl <=5, ]

head(mtcars)
