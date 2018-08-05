
y <- rnorm(n=36,mean=250,sd=2)
x <- sapply(y, function(y) rnorm(1,1.1*y,1))
write.table(round(x, digits=0), row.names = FALSE)

