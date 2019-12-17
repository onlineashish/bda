print(airquality)
print(dim(airquality))

sapply(airquality,class)

#testing the missing values
sum(is.na(airquality))
mean(is.na(airquality))

aq = airquality
aq_recoded = data.frame(sapply(aq,function(x) ifelse(is.na(x), mean(x,na.rm = TRUE),x)))
print(dim(aq_recoded))
sum(is.na(aq_recoded))
mean(is.na(aq_recoded))

#exlude missing values
new_aq = na.omit(airquality)
print(dim(new_aq))
sum(is.na(new_aq))
mean(is.na(new_aq))
