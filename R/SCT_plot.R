# Function to plot color bar
color.bar <- function(lut, min, max=-min, nticks=11, ticks=seq(min, max, len=nticks), title='') {
  scale = (length(lut)-1)/(max-min)
  plot(c(0,10), c(min,max), type='n', bty='n', xaxt='n', xlab='', yaxt='n', ylab='', main=title)
  axis(2, ticks, las=1)
  for (i in 1:(length(lut)-1)) {
    y = (i-1)/scale + min
    rect(0,y,10,y+1/scale, col=lut[i], border=NA)
  }
}


#' @title plot_sc_temp_merge
#' @description Create maps for quick visualizaton of aggregrated bottom temperature
#' @import ROracle lubridate DBI PBSmapping
#' @export
plot_sc_temp_merge <- function(){

  assign('shpfile', system.file("extdata", "801-bord_l.shp", package = "SCTemperature"), pkg.env)

  drv <- DBI::dbDriver("Oracle")

  con8 <<-
    ROracle::dbConnect(drv,
                       username = oracle.snowcrab.user,
                       password = oracle.snowcrab.password,
                       dbname = oracle.snowcrab.server)
  res <- ROracle::dbSendQuery(con8, "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
  res <- ROracle::dbSendQuery(con8, "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SSXFF'")
  res <- ROracle::dbSendQuery(con8, "ALTER SESSION SET  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SSXFF TZR'")
  res <- ROracle::dbReadTable(con8, "SC_TEMP_AGG")
  res$latitude = matrix(unlist(strsplit(res$POS, ",")), ncol = 2, byrow = T)[,1]
  res$longitude = matrix(unlist(strsplit(res$POS, ",")), ncol = 2, byrow = T)[,2]
  mint = floor(min(res$TEMP))
  maxt = ceiling(max(res$TEMP))
  ii <- cut(res$TEMP, breaks = seq(mint, maxt, len = 100),
            include.lowest = TRUE)
  res$col <- colorRampPalette(c("blue","green","yellow","red"))(99)[ii]

  legv = seq(mint, maxt, len = 10)
  legc = colorRampPalette(c("blue","green","yellow","red"))(10)
  da = split(res, res$WEEKSPAN)
  for(i in 1:length(da)){
    das = da[[i]]
    das$PID = 1:nrow(das)
    das$X = as.numeric(das$longitude)
    das$Y = as.numeric(das$latitude)
    png(file = file.path("C:","bio.data", "bio.snowcrab","maps", "temp_merge_plots", paste(das$WEEKSPAN[1], ".png", sep = "")), width = 800, height = 600)

    shpps = importShapefile(pkg.env$shpfile)
    plotLines(shpps, xlim = c(-70,-55), ylim = c(41.5,49), main = das$WEEKSPAN[1], col = "snow1", cex = .5)

    dx = as.PolyData(das, projection = NULL, zone = NULL)

    addPoints(dx, col = dx$col, pch = 20, main = das$WEEKSPAN[1])
    legend("topright", legend = round(legv,1), col = legc, pch = 20, cex = 2)
    dev.off()
  }
}

