#a
n <- as.integer(readline(prompt = "Enter no of Employee:\t")) 

EmpId <- vector(mode="character", length= n) 
EmpName <- vector(mode="character", length= n) 
DOJ <- vector(mode="character", length= n) 
EmpCode <- vector(mode="numeric",length = n) 
Desig <- vector(mode="character",length = n) 
Dept <- vector(mode="character",length = n) 

print("Enter EmpId") 
for (i in 1:n)   
  EmpId[i] <- as.character(readline()) 

print("Enter Employee Name") 
for (i in 1:n)   
  EmpName [i] <- readline() 

print("Enter DOJ" ) 
for (i in 1:n)   
  DOJ[i] <- (readline()) 

print("Enter EmployeeCode" ) 
for (i in 1:n)   
  EmpCode[i] <- as.integer(readline()) 

print("Enter Designation" ) 
for (i in 1:n)   
  Desig[i] <- (readline()) 

print("Enter Dept" ) 
for (i in 1:n)   
  Dept[i] <- (readline())

#b
Emp <- data.frame(EmpId,EmpName,EmpCode,Desig,Dept,DOJ) 

print("The Employee detials are as follows") 
print(Emp)

#c
write.csv(Emp,"Empfile.csv") 

#d
readStudent=read.csv("Empfile.csv") 

#e
print("Enter a new row") 
u<- readline(prompt = "EmpId:\t") 
n<- readline(prompt = "EmpName:\t") 
m<- readline(prompt = "EmpCode:\t") 
A<- readline(prompt = "Desig:\t") 
s<- readline(prompt = "Dept:\t") 
t<- readline(prompt = "DOJ:\t") 

Emp<- data.frame(u,n,m,A,s,t) 

write.table(x,"Empfile.csv",col.names = FALSE,  append = T,row.names = T, quote= FALSE, sep = ",")

Emp
