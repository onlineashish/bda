#a.
n <- as.integer(readline(prompt = "Enter no of students:\t")) # Read No of students

# Declare the vector of character of length n 
USN <- vector(mode="character", length= n)  
Name <- vector(mode="character", length= n) 
Marks <- vector(mode="numeric", length= n)

#Read the elements of the vector 
print("Enter USN") 
for (i in 1:n)
  USN[i] <- as.character(readline())

print("Enter Name")
for (i in 1:n)
  Name [i] <- readline()

print("Enter Marks" )
for (i in 1:n)
  Marks[i] <- as.integer(readline())

#Construct the data frame from the vectors 
student <- data.frame(USN,Name,Marks) 
print("The Student detials are as follows") 
print(student) # Display data frame

#b.
print("Enter Age")  # Read the vector of Age 
Age <- vector(mode="integer", length=n) 
for (i in 1:n)   
  Age [i] <- as.integer(readline()) 
student <- cbind(student,Age) # Append the vector to the data frame 
print(student)

#c.
for(i in 1:n) # Print student age < 20 , marks > 25   
  if ( student[i,][3]  > 25 )     
    if (student[i,][4] < 20)               
      print(student[i,])
