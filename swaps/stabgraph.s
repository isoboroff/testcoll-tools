# attach(stab);

stabgraph <- function(stab, measure, main.title) {

  panel.stability <- function(x, y, subscripts, my.data, ...) {
    panel.grid(h = -1, v = 5);
    panel.xyplot(x, y, xlim=c(0,50), ylim=c(0,0.5), ...);
    panel.abline(h=0.05);
    m <- nls(err ~ A1 * exp(-1 * A2 * qss),
             start = list(A1=1, A2=0.25),
             data = my.data,
             subset = subscripts);
    srt <- sortedXyData(my.data$qss[subscripts], fitted(m));
    xint <- NLSstClosestX(srt, 0.05);
    llines(my.data$qss[subscripts], fitted(m), col="red");
    ltext(c(49, 49, 49, 49), c(0.40, 0.35, 0.3, 0.25), adj=1, col="red",
          labels = c(paste(names(coef(m)), signif(coef(m), digits=3)),
            paste("ResErr", signif(summary(m)$sigma, digits=3)),
            paste("5\%int", signif(xint, digits=3))));
  }

  make.plot <- function(my.data, measure, main.title) {
    bin.labels <- paste(">", seq(0, .2, by=0.01))
    bin.lab <- factor(my.data$bin, labels=bin.labels)
    
    tb.plot <- xyplot(err ~ qss | bin.lab,
                      panel=panel.stability,
                                        # layout=c(6,1),
                      subset=(bin != 0 & meas == measure),
                      xlab="Query set size",
                      ylab="Error rate",
                      xlim=c(0,50),
                      main=main.title,
                      par.settings=col.whitebg(),
                      as.table=TRUE,
                      data=my.data,
                      my.data=my.data
                      )
    print(tb.plot)
  }

  make.plot(stab, measure, main.title)
}

# make.plot("bpref")
# print(np.plot, position=c(0,0.5,1,1), more=T)

# detach(stab)
