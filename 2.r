#a.
# Create the vector of strings
x<-c("Control", "Control", "Control", "Ear Removal", "Ear Removal", "Ear Removal", "Ear Removal", "Fake Ear Removal", "Fake Ear Removal", "Fake Ear Remov al", "Fake Ear Removal")
# display the vector 
x

#construct factor from the vector 
xfact<- factor(x)

#Display the vector
xfact

nlevels(xfact)

#b.
x<-c(rep("a",25),rep("b",15),rep("c",58))
x

length(x)  # Find the length of the vector

table1<- data.frame(x) # Construct table from the vector
table1
