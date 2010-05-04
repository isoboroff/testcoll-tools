# attach(stab);

vbgraph.extrap <- function(stab, measure, main.title) {

  make.plot <- function(my.data, measure, main.title) {
    bin.labels <- paste(">", seq(0, .2, by=0.01))
    bin.lab <- factor(my.data$bin, labels=bin.labels)
    
    tb.plot <- xyplot(err ~ qss,
                      groups=bin,
                      panel=function(x, y, subscripts, groups, ...) {
                        panel.superpose(x, y, subscripts, groups, ...);
                        panel.abline(h=0.05)
                      },
                      panel.groups=function(x,y,subscripts,groups,...) {
                        panel.xyplot(x, y, xlim=c(0, 50),
                                     type=c("p","l","g"), ...)
                        m <- nls(err ~ A1 * exp(-1 * A2 * qss),
                                 start = list(A1=1, A2=0.25),
                                 data = my.data,
                                 subset = subscripts);
                        srt <- sortedXyData(my.data$qss[subscripts],
                                            fitted(m));
                        xint <- NLSstClosestX(srt, 0.05);
                        llines(my.data$qss[subscripts], fitted(m), col="red");
                        llines(25:50, predict(m, list(qss=25:50)), col="blue");
                      },
                      auto.key=list(text=levels(bin.lab)[1:10], lines=T,
                        space="right"),
                      subset=(bin != 0 & bin < 10 & meas == measure),
                      xlab="Query set size",
                      ylab="Error rate",
                      xlim=c(0, 50),
                      main=main.title,
                      par.settings=col.whitebg(),
                      data=my.data,
                      )
    print(tb.plot)
  }

  make.plot(stab, measure, main.title)
}

# make.plot("bpref")
# print(np.plot, position=c(0,0.5,1,1), more=T)

# detach(stab)
