

pkg.env = new.env(parent = emptyenv())
assign('con', NULL, pkg.env)
assign('M_UID', NULL, pkg.env)
assign('T_UID', NULL, pkg.env)
assign('GlobLat', NULL, pkg.env)
assign('GlobLon', NULL, pkg.env)
assign('GlobDep', NULL, pkg.env)
assign('tz', "America/Halifax", pkg.env)
assign('testwrite', TRUE, pkg.env)


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


#' @title regenStationInventory
#' @description Update station inventory file that is used to help inform ranges using climate history
#' @export
regenStationInventory  = function() {
  assign('metafile', system.file("extdata", "Station Inventory EN.csv", package = "SCTemperature"), pkg.env)


  url <- paste0(
    "ftp://client_climate@ftp.tor.ec.gc.ca/",
    "Pub/Get_More_Data_Plus_de_donnees/",
    "Station%20Inventory%20EN.csv"
  )
  SI <- readLines(url)
  write.csv(
    SI,
    pkg.env$metafile,
    row.names = F,
    quote = F
  )
}

#' @title Populate
#' @description  The prefered method of entry. Writes temperature data to oracle database from files that have formatted header information. See description file for header format information.
#' @param fn The folder path that contains the temperature files to write
#' @param test If true clear test tables and writes results for review, If False writes to main tables.
#' @import ROracle lubridate DBI
#' @return TRUE on success
#' @export
Populate = function(fn = NA, test = T) {

  if(test) Sys.setenv(testwrite = T)
  else Sys.setenv(testwrite = F)
  Sys.setenv(TZ = "America/Halifax")
  Sys.setenv(ORA_SDTZ = "America/Halifax")
  drv <- DBI::dbDriver("Oracle")
  if (is.na(fn)) {
    yr = as.character(lubridate::year(Sys.Date()))
    fn = file.path(
      "R:",
      "Science",
      "Population Ecology Division",
      "Shared",
      "Snowcrab",
      "TemperatureData",
      paste("Entry ", yr, sep = "")
    )
  }
  fl = list.files(
    fn,
    full.names = T,
    recursive = F,
    include.dirs = FALSE
  )
  fl = fl[which(!grepl("ReceiverPlots", fl))]


  #### Get list of data already added to database ####
  con8 <<-
    ROracle::dbConnect(drv,
                       username = oracle.snowcrab.user,
                       password = oracle.snowcrab.password,
                       dbname = oracle.snowcrab.server)

  if(test){
    ROracle::dbSendQuery(con8, "TRUNCATE TABLE SC_TEMPERATURE_META_TEST")
    ROracle::dbSendQuery(con8, "TRUNCATE TABLE SC_TEMPERATURE_BASE_TEST")
  }

  res <-
    ROracle::dbSendQuery(con8, "select DISTINCT FILE_S from SC_TEMPERATURE_META")
  res <- ROracle::fetch(res, n = -1)
  FILE_S <- res$FILE_S

  res2 <-
    ROracle::dbSendQuery(con8, "select DISTINCT PID from SC_TEMPERATURE_META")
  res2 <- ROracle::fetch(res2, n = -1)
  PID <- res2$PID


  if (is.null(pkg.env$M_UID)) {
    res <-
      ROracle::dbSendQuery(con8, "select DISTINCT M_UID from SC_TEMPERATURE_META")
    res <- ROracle::fetch(res, n = -1)
    #M_UID <<- res$M_UID
    # M_UID <<- c(M_UID, UID)
    assign('M_UID', res$M_UID, pkg.env)
  }

  if (is.null(pkg.env$T_UID)) {
    res2 <-
      ROracle::dbSendQuery(con8, "select DISTINCT T_UID from SC_TEMPERATURE_BASE")
    res2 <- ROracle::fetch(res2, n = -1)
    #T_UID <<- res2$T_UID
    assign('T_UID', res2$T_UID, pkg.env)
  }
  ROracle::dbDisconnect(con8)

  #####################################################
  #Loop through all files in directory
  for (i in 1:length(fl)) {
    #Only look at files that havent yet been added
    if (!fl[i] %in% FILE_S) {
      print(fl[i])

      head = readLines(fl[i], n = 50)

      ind = grep("<SCHEADER>", head)
      ind2 = grep("</SCHEADER>", head)
      #Check if file is standard modified SCTemp file with nessesary headers
      if (length(ind) == 0) {
        #If not we will attempt to process based on known formats (so far receiver data)
        manualmeta = read.csv(fl[i])
        #Verify if we are dealing with receiver data
        if (all(
          names(manualmeta) == c(
            "station_name",
            "date",
            "receiver",
            "description",
            "data",
            "units",
            "rcv_serial_no",
            "deploy_date",
            "recover_date",
            "recover_ind",
            "dep_lat",
            "dep_long",
            "the_geom",
            "catalognumber"
          )
        )) {
          acoustic_file_handler(data = manualmeta, fl[i])
        }
        else{
          print("Unknown format")
        }

      }
      else{
        head = head[ind:ind2]
        head = paste0(head, collapse = " ")

        DEPTH = NA
        DEPTHUNITS = "meters"
        LAT = NA
        LON = NA
        SDATE = NA
        EDATE = NA
        PORT = NA
        LOCATION = NA
        PROJECT = NA
        NOTES = NA
        USERID = NA
        DTZ = pkg.env$tz


        if (grepl("<LAT>",  head))
          LAT = unlist(strsplit(trimws(
            substr(
              head,
              regexpr("<LAT>", head)[1] + 5,
              regexpr("</LAT>", head)[1] - 1
            )
          ), ","))
        if (grepl("<LON>",  head))
          LON = unlist(strsplit(trimws(
            substr(
              head,
              regexpr("<LON>", head)[1] + 5,
              regexpr("</LON>", head)[1] - 1
            )
          ), ","))
        if (grepl("<STARTDATE>",  head))
          SDATE = unlist(strsplit(trimws(
            substr(
              head,
              regexpr("<STARTDATE>", head)[1] + 11,
              regexpr("</STARTDATE>", head)[1] - 1
            )
          ), ","))
        if (grepl("<ENDDATE>",  head))
          EDATE = unlist(strsplit(trimws(
            substr(
              head,
              regexpr("<ENDDATE>", head)[1] + 9,
              regexpr("</ENDDATE>", head)[1] - 1
            )
          ), ","))
        if (grepl("<DEPTH>",  head))
          DEPTH = unlist(strsplit(trimws(
            substr(
              head,
              regexpr("<DEPTH>", head)[1] + 7,
              regexpr("</DEPTH>", head)[1] - 1
            )
          ), ","))
        if (grepl("<DEPTHUNITS>",  head))
          DEPTHUNITS = unlist(strsplit(trimws(
            substr(
              head,
              regexpr("<DEPTHUNITS>", head)[1] + 12,
              regexpr("</DEPTHUNITS>", head)[1] - 1
            )
          ), ","))

        if (!is.na(DEPTH)) {
          if (tolower(DEPTHUNITS) == "fathoms")
            DEPTH = as.character(as.numeric(DEPTH) * 1.8288)
          if (tolower(DEPTHUNITS) == "feet")
            DEPTH = as.character(as.numeric(DEPTH) * .3048)
        }

        if (grepl("<TZ>",  head))
          DTZ = unlist(strsplit(trimws(
            substr(
              head,
              regexpr("<TZ>", head)[1] + 4,
              regexpr("</TZ>", head)[1] - 1
            )
          ), ","))

        if (length(LAT) == length(LON) &&
            length(EDATE) == length(SDATE)) {
          if (grepl("<PORT>",  head))
            PORT = unlist(strsplit(trimws(
              substr(
                head,
                regexpr("<PORT>", head)[1] + 6,
                regexpr("</PORT>", head)[1] - 1
              )
            ), ","))
          if (grepl("<LOCATION>",  head))
            LOCATION = unlist(strsplit(trimws(
              substr(
                head,
                regexpr("<LOCATION>", head)[1] + 10,
                regexpr("</LOCATION>", head)[1] - 1
              )
            ), ","))
          if (grepl("<PROJECT>",  head))
            PROJECT = unlist(strsplit(trimws(
              substr(
                head,
                regexpr("<PROJECT>", head)[1] + 9,
                regexpr("</PROJECT>", head)[1] - 1
              )
            ), ","))
          if (grepl("<NOTES>",  head))
            NOTES = unlist(strsplit(trimws(
              substr(
                head,
                regexpr("<NOTES>", head)[1] + 7,
                regexpr("</NOTES>", head)[1] - 1
              )
            ), ","))

          if (grepl("<USERID>",  head))
            USERID = unlist(strsplit(trimws(
              substr(
                head,
                regexpr("<USERID>", head)[1] + 8,
                regexpr("</USERID>", head)[1] - 1
              )
            ), ","))

          if (is.na(USERID))
            USERID = "UNK"

          xx = gsub(USERID, "", PID[grep(USERID, PID)])
          if (length(xx) == 0) {
            xx = "001"
          }
          else{
            xx = as.character(max(as.numeric(gsub(
              USERID, "", PID[grep(USERID, PID)]
            ))) + 1)
            while (nchar(xx) < 3)
              xx = paste("0", xx, sep = "")
          }
          xx = paste(USERID, xx, sep = "")
          #GlobLat <<- LAT
          assign('GlobLat', LAT, pkg.env)
          #GlobLon <<- LON
          assign('GlobLon', LON, pkg.env)
          #GlobDep <<- DEPTH
          assign('GlobDep', DEPTH, pkg.env)

          for (j in 1:length(SDATE)) {
            AddTempMetadata(
              PID = xx,
              UID = paste(xx, j, sep = "-"),
              Project = PROJECT,
              Location = LOCATION,
              RecorderType = NA,
              RecorderID = NA,
              StartDate = lubridate::dmy_hms(SDATE[j], tz = DTZ),
              EndDate =  lubridate::dmy_hms(EDATE[j], tz = DTZ),
              RecordingRate = NA,
              Port = PORT,
              File = fl[i],
              LAT.DD = LAT[j],
              LON.DD = LON[j],
              Observed_Depth = DEPTH[j],
              UnitsforTemp = NA,
              QAQC = NA,
              Notes = NOTES,
              HaulDate_Start =  lubridate::dmy_hms(SDATE[j], tz = DTZ),
              HaulDate_End =  lubridate::dmy_hms(EDATE[j], tz = DTZ)
            )

          }
          PID[length(PID) + 1] = xx
        }
        else{
          print(paste(fl[i], " Has position AND OR date mismatch", sep = ""))
        }
      }
    }
    else{
      print(paste(fl[i], " Already exists in Database", sep = ""))
    }
  }
  return(TRUE)
}

#' @title click.temp
#' @description Internal function. Further refine the range of when the sensor is at bottom by clicking start and end times on a plot.
#' @param da The data to plot
#' @param af The subset of data that is code informed
#' @param uid The unique identifier for plotting
#' @import lubridate tcltk
#' @return data at new range
#' @export
click.temp = function(da = NA,
                      af = NA,
                      uid = NA) {

  #Set up plot range
  if ((nrow(af) / nrow(da)) < .05) {
    sdate = af$T_DATE[1] - days(5)
    edate = af$T_DATE[length(af$T_DATE)] + days(5)
    if (sdate > da$T_DATE[1])
      da = da[which(da$T_DATE >= sdate),]
    if (edate < da$T_DATE[length(da$T_DATE)])
      da = da[which(da$T_DATE <= edate),]
  }
  yrange <- range(c(da$TEMP, da$TEMP + 4, da$TEMP - 4))

  xy <- xy.coords(da$T_DATE, da$TEMP)
  xval <- xy$x
  yval <- xy$y

  result = NULL

  active = T
  while (active) {
    x11(25, 15)
    par(mar = c(5, 8, 4, 4) + 0.1)
    plot(
      da$T_DATE,
      da$TEMP,
      axes = FALSE,
      ylim = yrange,
      xlab = 'Date',
      ylab = 'Temperature',
      type = "l",
      main = uid,
      lwd = 1
    )

    axis.POSIXct(1, at = seq(da$T_DATE[1], da$T_DATE[length(da$T_DATE)], by = "month"))
    axis(2)
    #Plot the filter
    lines(
      af$T_DATE,
      af$TEMP,
      axes = F,
      type = "l",
      col = "red",
      lwd = 1
    )
    points(af$T_DATE[1], af$TEMP[1], pch = 3, col = "red")
    points(af$T_DATE[length(af$T_DATE)], af$TEMP[length(af$TEMP)], pch = 3, col = "red")

    xy <- xy.coords(da$T_DATE, da$TEMP)
    x <- xy$x
    y <- xy$y

    id = identify(x, y, n = 1, pos = TRUE)
    start = da$T_DATE[id$ind]

    points(xval[id$ind], yval[id$ind], col = "darkgreen", pch = 19)
    #Code that allow identifying clicks
    id2 = identify(x, y, n = 1, pos = TRUE)
    end = da$T_DATE[id2$ind]
    points(xval[id2$ind], yval[id2$ind], col = "darkgreen", pch = 19)

    #   userinp <- readline("Are you happy with these values? (Y or N)"  )

    res <- tkmessageBox(
      title = "ManualTD",
      message = "Are you happy with these values?",
      icon = "info",
      type = "yesno"
    )

    #if(userinp == "Y" || userinp == "y"){
    if (grepl("yes", res)) {
      if (start > end) {
        etmp = end
        end = start
        start = etmp
      }

      row = c(start, end)
      if (is.null(result))
        result = data.frame(start = start, end = end)
      else
        result = rbind(result, data.frame(start = start, end = end))

      # res2 <- tkmessageBox(
      #   title = "ManualTD",
      #   message = "Any more?",
      #   icon = "info",
      #   type = "yesno"
      # )
      # if (grepl("yes", res2)) {
      #   active = T
      #   dev.off()
      # }
      # if (grepl("no", res2)) {
         active = F
         dev.off()
      # }

    }
    #if(userinp == "N" || userinp == "n"){
    if (grepl("no", res)) {
      active = T
      dev.off()
    }
  }


  return(result)
}


#' @title Populate_by_worksheet
#' @description  Alternative method of entry. Writes temperature data to oracle database from a worksheet that contains the needed information.
#' @param fn The csv file that contains metadata information nessesary for writting temperature data
#' @import ROracle lubridate
#' @return TRUE on success
#' @export
Populate_by_worksheet = function(fn) {


  Sys.setenv(TZ = "America/Halifax")
  Sys.setenv(ORA_SDTZ = "America/Halifax")
  manualmeta = read.csv(fn)
  mmpid = split(manualmeta, manualmeta$ProjectID)
  for (i in 1:length(mmpid)) {
    mm_sub = mmpid[[i]]
    for (j in 1:nrow(mm_sub)) {
      m_ind = mm_sub[j,]
      if (!grepl("no FSRS", m_ind$Notes)) {
        if (!grepl("was", m_ind$ProjectID)) {
          m_ind[which(is.na(m_ind))] = NA
          m_ind[which(m_ind == "")] = NA

          AddTempMetadata(
            PID = m_ind$ProjectID,
            UID = paste(m_ind$ProjectID, j, sep = "-"),
            Project = m_ind$Project,
            Location = m_ind$Location,
            RecorderType = m_ind$RecorderType,
            RecorderID = m_ind$RecorderID,
            StartDate = dmy(m_ind$StartDate),
            EndDate = dmy(m_ind$EndDate),
            RecordingRate = m_ind$RecordingRate,
            Port = m_ind$Port,
            File = m_ind$File,
            LAT.DD = m_ind$LAT.Decimal.Degrees,
            LON.DD = m_ind$LON.Decimal.Degrees,
            Observed_Depth = m_ind$Observed_Depth,
            UnitsforTemp = m_ind$UnitsforTemp,
            QAQC = m_ind$QAQC,
            Notes = m_ind$Notes,
            HaulDate_Start = dmy(m_ind$HaulDate_Start),
            HaulDate_End = dmy(m_ind$HaulDate_End)
          )

        }
      }

    }
  }
  return(TRUE)
}



#' @title  AddTempMetadata
#' @description  Internal function. Writes metadata to database
#' @import ROracle DBI
#' @export
AddTempMetadata = function(PID = NULL,
                           UID = NULL,
                           Project = NA,
                           Location = NA,
                           RecorderType = NA,
                           RecorderID = NA,
                           StartDate = NA,
                           EndDate = NA,
                           RecordingRate = NA,
                           Port = NA,
                           File = NA,
                           LAT.DD = NA,
                           LON.DD = NA,
                           Observed_Depth = NA,
                           UnitsforTemp = NA,
                           QAQC = "",
                           Notes = NA,
                           HaulDate_Start = NA,
                           HaulDate_End = NA) {

  Sys.setenv(TZ = "America/Halifax")
  Sys.setenv(ORA_SDTZ = "America/Halifax")
  drv = DBI::dbDriver("Oracle")
  con2 =  ROracle::dbConnect(drv,
                       username = oracle.snowcrab.user,
                       password = oracle.snowcrab.password,
                       dbname = oracle.snowcrab.server)

  if (is.null(pkg.env$M_UID)) {
    res = ROracle::dbSendQuery(con2, "select DISTINCT M_UID from SC_TEMPERATURE_META")
    res = ROracle::fetch(res, n = -1)
    #M_UID <<- res$M_UID
    assign('M_UID', res$M_UID, pkg.env)

  }

  if (is.null(pkg.env$T_UID)) {
    res2 <-
      ROracle::dbSendQuery(con2, "select DISTINCT T_UID from SC_TEMPERATURE_BASE")
    res2 <- ROracle::fetch(res2, n = -1)
    #T_UID <<- res2$T_UID
    assign('T_UID', res2$T_UID, pkg.env)

  }

  ROracle::dbDisconnect(con2)


  dx = data.frame(
    as.character(UID),
    as.character(PID),
    as.character(Project),
    as.character(Location),
    as.character(RecorderType),
    as.character(RecorderID),
    StartDate,
    EndDate,
    RecordingRate,
    as.character(Port),
    as.character(File),
    LAT.DD,
    LON.DD,
    as.numeric(as.character(Observed_Depth)),
    UnitsforTemp,
    QAQC,
    as.character(Notes),
    HaulDate_Start,
    HaulDate_End,
    stringsAsFactors = F
  )
  names(dx) = c(
    "M_UID",
    "PID",
    "PROJECT",
    "LOCATION",
    "RECORDER_TYPE",
    "RECORDER_ID",
    "START_DATE",
    "END_DATE",
    "RECORDING_RATE",
    "PORT",
    "FILE_S",
    "M_LAT_DD",
    "M_LON_DD",
    "OBSERVED_DEPTH_METERS",
    "UNITS_FOR_TEMP",
    "QAQC",
    "NOTES",
    "HAUL_DATE_START",
    "HAUL_DATE_END"
  )


  if (!UID %in% pkg.env$T_UID) {
    AddTempRawdata(File,
                   UID,
                   PID,
                   LAT.DD,
                   LON.DD,
                   HaulDate_Start,
                   HaulDate_End,
                   dx)
    #T_UID <<- c(T_UID, UID)
    assign('T_UID', c(pkg.env$T_UID, UID), pkg.env)
  }

  if (!UID %in% pkg.env$M_UID) {
    con3 = ROracle::dbConnect(drv,
                         username = oracle.snowcrab.user,
                         password = oracle.snowcrab.password,
                         dbname = oracle.snowcrab.server)
    if(pkg.env$testwrite){

      ROracle::dbWriteTable(
        con3,
        name = "SC_TEMPERATURE_META_TEST",
        append = T,
        row.names = F,
        value = dx
      )
    }else{
    ROracle::dbWriteTable(
      con3,
      name = "SC_TEMPERATURE_META",
      append = T,
      row.names = F,
      value = dx
    )
    }
   # M_UID <<- c(M_UID, UID)
    assign('M_UID',  c(pkg.env$M_UID, UID), pkg.env)
    ROracle::dbDisconnect(con3)
  }



}
#' @title  AddTempRawdata
#' @description  Internal function. Writes rawdata to database
#' @import ROracle DBI
#' @export
AddTempRawdata = function(fn = NULL,
                          UID = NULL,
                          ProjectId = NULL,
                          Lat = NULL,
                          Lon = NULL,
                          HaulDate_Start = NULL,
                          HaulDate_End = NULL,
                          metadx = NULL) {

  Sys.setenv(TZ = "America/Halifax")
  Sys.setenv(ORA_SDTZ = "America/Halifax")
  print("Add Raw Data")
  drv = DBI::dbDriver("Oracle")
  con4 = ROracle::dbConnect(drv,
                       username = oracle.snowcrab.user,
                       password = oracle.snowcrab.password,
                       dbname = oracle.snowcrab.server)

  autof = F

  if (is.null(pkg.env$M_UID)) {
    res = ROracle::dbSendQuery(con4, "select DISTINCT M_UID from SC_TEMPERATURE_META")
    res = ROracle::fetch(res, n = -1)
    #M_UID <<- res$M_UID
    assign('M_UID', res$M_UID, pkg.env)

  }

  if (is.null(pkg.env$T_UID)) {
    res2 = ROracle::dbSendQuery(con4, "select DISTINCT T_UID from SC_TEMPERATURE_BASE")
    res2 = ROracle::fetch(res2, n = -1)
    #T_UID <<- res2$T_UID
    assign('T_UID', res2$T_UID, pkg.env)

  }
  ROracle::dbDisconnect(con4)

  if (!file.exists(fn)) {
    allfilelist = list.files(path = raw.dir,
                             full.names = T,
                             recursive = TRUE)

    allfilelist = allfilelist[grep(ProjectId, allfilelist)]
    read = 1
    sub = NULL
    subs = NULL
    sube = NULL
    if (length(allfilelist) > 1) {
      warning(paste(
        "More than one raw file name match for projectID: ",
        ProjectId,
        sep = ""
      ))
      rea = which(grepl(".csv", allfilelist))
      if (length(rea) > 0)
        read = rea
    }

    if (grepl("CTS", metadx$PROJECT)) {
      allfilelist = list.files(path = raw.dir,
                               full.names = T,
                               recursive = TRUE)

      ctsmat =  paste("CTS", ".", unlist(strsplit(metadx$PORT, "CTS"))[2], sep = "")
      allfilelist = allfilelist[grep(ctsmat, allfilelist)]
      sub = metadx$RECORDER_ID
      subs = metadx$HAUL_DATE_START
      sube = metadx$HAUL_DATE_END
    }
  }
  else{
    allfilelist = fn
    read = 1
    sub = NULL
  }
  if (length(allfilelist) < 1) {
    stop(paste(
      "There was no matching raw data file for projectID: ",
      ProjectId,
      sep = ""
    ))
  }
  else{
    sta_cont = NULL

    afdat = NULL
    madat = NULL
    if (!UID %in% pkg.env$T_UID) {
      sta_cont = standardize.temp(
        allfilelist[read],
        uid = UID,
        lat = Lat,
        lon = Lon,
        subset = sub
      )
      if (is.na(HaulDate_Start))
        HaulDate_Start = NULL
      if (is.na(HaulDate_End))
        HaulDate_End = NULL
      if (!is.null(HaulDate_End)) {
        madat = trim_to_dates(sta_cont$data, HaulDate_Start,	HaulDate_End, TRUE)
        if (nrow(madat) == 0) {
          madat = auto_filter(
            da = sta_cont$data,
            lat = Lat,
            lon = Lon,
            uid = UID
          )
          sta_cont$QAQC = paste(sta_cont$QAQC, "Auto_Filtered", sep = " ")
        }
        else{
          sta_cont$QAQC = paste(sta_cont$QAQC, "UserDef_Informed", sep = " ")
        }
      }
      else{
        madat = auto_filter(
          da = sta_cont$data,
          lat = Lat,
          lon = Lon,
          uid = UID
        )
        sta_cont$QAQC = paste(sta_cont$QAQC, "Code_Informed", sep = " ")

      }
      dx = sta_cont$data
      rx = click.temp(dx, madat, UID)
      sta_cont$QAQC = paste(sta_cont$QAQC, "Plot_Clicked_Filtered", sep = " ")
      dx = trim_to_dates(dx, rx$start,	rx$end, FALSE)
      sta_cont$deployments = nrow(rx)

      drv = DBI::dbDriver("Oracle")

      con5 = ROracle::dbConnect(drv,
                           username = oracle.snowcrab.user,
                           password = oracle.snowcrab.password,
                           dbname = oracle.snowcrab.server)

      if(pkg.env$testwrite){

        ROracle::dbWriteTable(
          con5,
          name = "SC_TEMPERATURE_BASE_TEST",
          append = T,
          row.names = F,
          value = dx
        )
      }else{
      ROracle::dbWriteTable(
        con5,
        name = "SC_TEMPERATURE_BASE",
        append = T,
        row.names = F,
        value = dx
      )
      }
      alluid = paste0(unique(dx$T_UID))
     # T_UID <<- c(T_UID, alluid)
      assign('T_UID', c(pkg.env$T_UID, alluid), pkg.env)
      ROracle::dbDisconnect(con5)
    }

    ##Will not get executed untill auto functions complete

    for (k in 1:sta_cont$deployments) {
      UID = alluid[k]

      if (!UID %in% pkg.env$M_UID) {
        ##Select which is of greater importance


        if (!is.null(metadx$PID))
          sta_cont$PID = metadx$PID

        if (!is.na(metadx$PROJECT))
          sta_cont$Project = metadx$PROJECT

        if (!is.na(metadx$LOCATION))
          sta_cont$Location = metadx$LOCATION

        if (!is.na(metadx$RECORDER_TYPE))
          sta_cont$RecorderType = metadx$RECORDER_TYPE

        if (!is.na(metadx$RECORDER_ID))
          sta_cont$RecorderID = metadx$RECORDER_ID

        if (!is.na(metadx$PORT)) {
          sta_cont$Port = metadx$PORT
        } else if (is.null(sta_cont$Port)) {
          sta_cont$Port = NA
        }

        if (!is.na(metadx$FILE_S)) {
          sta_cont$File = metadx$FILE_S
        } else if (is.null(sta_cont$File)) {
          sta_cont$File = NA
        }

        if (!is.na(metadx$M_LAT_DD))
          sta_cont$LAT.DD = metadx$M_LAT_DD

        if (!is.na(metadx$M_LON_DD))
          sta_cont$LON.DD = abs(as.numeric(metadx$M_LON_DD)) * -1

        if (!is.na(metadx$OBSERVED_DEPTH_METERS)) {
          sta_cont$Observed_Depth = metadx$OBSERVED_DEPTH_METERS
        } else if (is.null(sta_cont$Observed_Depth)) {
          sta_cont$Observed_Depth = NA
        }


        if (is.null(sta_cont$RecordingRate))
          sta_cont$RecordingRate = metadx$RECORDING_RATE
        if (is.null(sta_cont$UnitsforTemp))
          sta_cont$UnitsforTemp = metadx$UNITS_FOR_TEMP
        if (is.null(sta_cont$StartDate))
          sta_cont$StartDate = metadx$START_DATE
        if (is.null(sta_cont$EndDate))
          sta_cont$EndDate = metadx$END_DATE

        rqaqc = ""
        if (!is.na(metadx$QAQC))
          rqaqc = trimws(paste(rqaqc, metadx$QAQC, sep = " "))
        if (!is.na(sta_cont$QAQC))
          rqaqc = trimws(paste(rqaqc, sta_cont$QAQC, sep = " "))

        rnotes = ""

        if (!is.na(metadx$NOTES))
          rnotes = trimws(paste(rnotes, metadx$NOTES, sep = " "))
        if (!is.na(sta_cont$Notes))
          rnotes = trimws(paste(rnotes, sta_cont$Notes, sep = " "))

        AddTempMetadata(
          PID = sta_cont$PID,
          UID = UID,
          Project = sta_cont$Project,
          Location = sub(",,", "", sta_cont$Location),
          RecorderType = sta_cont$RecorderType,
          RecorderID = sta_cont$RecorderID,
          StartDate = sta_cont$StartDate,
          EndDate = sta_cont$EndDate,
          RecordingRate = sub(",,", "", sta_cont$RecordingRate),
          Port = sta_cont$Port,
          File = sub(",,", "", sta_cont$File),
          LAT.DD = sta_cont$LAT.DD,
          LON.DD = sta_cont$LON.DD,
          Observed_Depth = sta_cont$Observed_Depth,
          UnitsforTemp = sta_cont$UnitsforTemp,
          QAQC = rqaqc,
          Notes = sta_cont$Notes,
          HaulDate_Start = rx$start[k],
          HaulDate_End = rx$end[k]
        )
       # M_UID <<- c(M_UID, UID)
        assign('M_UID', c(pkg.env$M_UID, UID), pkg.env)
      }

    }

  }

}

#' @title standardize.temp
#' @description  Internal function. Convert various data inputs such as minilog and seabird to a common standard used for this project. This method should be build upon for any new data streams
#' @param fn The file with sensordata to read
#' @param uid the unique identifier if known
#' @param lat latitude if known
#' @param lon longitude if known
#' @param subset subset by Serial_number for CTS data
#' @import lubridate
#' @export
standardize.temp = function(fn,
                            uid = NULL,
                            lat = NULL,
                            lon = NULL,
                            subset = NULL) {

  ret = NULL

  ret$RecordingRate = NA
  ret$UnitsforTemp = NA
  ret$StartDate = NULL
  ret$EndDate = NULL
  ret$Notes = NA
  ret$Location = NA

  known = F
  header = readLines(fn, n = 50)
  if (any(grepl("<SCHEADER>", header)) &&
      any(grepl("</SCHEADER>", header))) {
    header = header[-(grep("<SCHEADER>", header):grep("</SCHEADER>", header))]
  }

  if (any(grepl("Minilog", header))) {
    miniold = F
    known = T


    ind = grep("Date\\(", readLines(fn, n = 50), ignore.case = TRUE)
    header = readLines(fn, n = ind)
    if (any(grepl("<SCHEADER>", header)) &&
        any(grepl("</SCHEADER>", header))) {
      header = header[-(grep("<SCHEADER>", header):grep("</SCHEADER>", header))]
    }
    header = gsub("\t", "", header)
    header = gsub("\\?C", "°C", header)
    header = gsub("\\?F", "°F", header)
    header = gsub(",,", "", header)

    if (any(grepl("* ", header, fixed = T)))
      miniold = TRUE

    data = read.csv(fn, skip = ind - 1)
    if (ncol(data) == 1)
      data = read.delim(fn, skip = ind - 1)


    #Expected to return data
    if (miniold)
      ret$Project = trimws(unlist(strsplit(header[grep("Study ID", header, ignore.case = TRUE)], "="))[2])
    else
      ret$Project = trimws(unlist(strsplit(header[grep("Study Description", header, ignore.case = TRUE)], ": "))[2])
    if (miniold) {
      ret$RecorderType = trimws(unlist(strsplit(header[grep("ID=Min", header, ignore.case = TRUE)], "="))[2])
      ret$RecorderID = trimws(unlist(strsplit(header[grep("Serial Number", header, ignore.case = TRUE)], "="))[2])
    }
    else{
      topars = trimws(unlist(strsplit(header[grep("Source Device", header, ignore.case = TRUE)], ":"))[2])
      topars = unlist(strsplit(topars, "-"))
      ret$RecorderType = paste(topars[1], topars[2], topars[3], sep = "-")
      ret$RecorderID = topars[4]
    }
    if (miniold)
      ret$StartDate = trimws(unlist(strsplit(header[grep("Start Time", header, ignore.case = TRUE)], "="))[2])
    else
      ret$StartDate = trimws(unlist(strsplit(header[grep("Study Start", header, ignore.case = TRUE)], ": "))[2])
    if (miniold)
      ret$EndDate = trimws(unlist(strsplit(header[grep("Finish Time", header, ignore.case = TRUE)], "="))[2])
    else
      ret$EndDate = trimws(unlist(strsplit(header[grep("Study Stop", header, ignore.case = TRUE)], ": "))[2])
    if (miniold)
      ret$RecordingRate = trimws(unlist(strsplit(header[grep("Sample Period", header, ignore.case = TRUE)], "="))[2])
    else
      ret$RecordingRate = trimws(unlist(strsplit(header[grep("Sample Interval", header, ignore.case = TRUE)], ": "))[2])
    if (miniold)
      ret$File = basename(fn)
    else
      ret$File = trimws(unlist(strsplit(header[grep("Source File", header, ignore.case = TRUE)], ": "))[2])

    ##Need to pull out UTC-3 or whatever and set to tz variable
    timediffutc = NULL
    tz = "America/Halifax"

    if (any(grepl("UTC", header, fixed = T))) {
      timediffutc = trimws(unlist(strsplit(header[grep("UTC", header, ignore.case = TRUE)], "UTC"))[2])
      timediffutc = gsub(")", "", timediffutc)
      timediffutc = gsub(",", "", timediffutc)
      timediffutc = as.numeric(timediffutc) * -1
      timediffutc = hours(timediffutc)

      tz = "UTC"

    }


    ret$UnitsforTemp = "Unknown"
    ##If fahrenheit we should convert
    if (any(grepl("°C", header, ignore.case = TRUE)))
      ret$UnitsforTemp = "Celsius"
    if (any(grepl("°F", header, ignore.case = TRUE)))
      ret$UnitsforTemp = "Fahrenheit"

    #NOT Expected to return data, but will try anyway
    ret$Port = trimws(unlist(strsplit(header[grep("Port", header, ignore.case = TRUE)], ": "))[2])
    LAT_DD = trimws(unlist(strsplit(header[grep("latitude", header, ignore.case = TRUE)], ": "))[2])
    LON_DD = trimws(unlist(strsplit(header[grep("longitude", header, ignore.case = TRUE)], ": "))[2])
    ret$Observed_Depth = trimws(unlist(strsplit(header[grep("Depth", header, ignore.case = TRUE)], ": "))[2])
    ret$QAQC = trimws(unlist(strsplit(header[grep("QAQC", header, ignore.case = TRUE)], ": "))[2])
    ret$Notes  = trimws(unlist(strsplit(header[grep("Notes", header, ignore.case = TRUE)], ": "))[2])


    if (length(ret$Port) == 0)
      ret$Port = NULL
    if (length(ret$Observed_Depth) == 0)
      ret$Observed_Depth = NULL
    if (length(ret$QAQC) == 0)
      ret$QAQC = ""
    if (length(ret$Notes) == 0)
      ret$Notes = ""
    if (length(ret$RecorderType) == 0)
      ret$RecorderType = NULL
    if (length(ret$RecorderID) == 0)
      ret$RecorderID = NULL


    ret$StartDate = ymd_hms(ret$StartDate)

    ret$EndDate = ymd_hms(ret$EndDate)

    #NORMALISE DATA
    #Convert date
    T_DATE = "NA"


    datecolumn = grep("date", names(data), ignore.case = T)
    if (any(grepl("yyyy-mm-dd", header, ignore.case = TRUE))) {
      timecolumn = grep("time", names(data), ignore.case = T)
      if (any(grepl("hh:mm:ss", header, ignore.case = TRUE))) {
        ds = data[, datecolumn][1]

        if (grepl("-", ds)) {
          if (is.null(timediffutc)) {
            T_DATE = lubridate::ymd_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = tz)
          }
          else{
            T_DATE = lubridate::ymd_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = tz)
            T_DATE = T_DATE + timediffutc
          }
        }
        if (grepl("/", ds)) {
          if (is.null(timediffutc)) {
            T_DATE = lubridate::dmy_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = tz)
          }
          else{
            T_DATE = lubridate::dmy_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = tz)
            T_DATE = T_DATE + timediffutc
          }
        }
      }
    }
    if (miniold)
      tempcolumn = 3
    else
      tempcolumn = grep("Temperature", names(data), ignore.case = T)

    TEMP = as.numeric(data[, tempcolumn])
    if (ret$UnitsforTemp == "Fahrenheit") {
      TEMP = ((TEMP - 32) * 5) / 9
      ret$UnitsforTemp = "Celsius"
    }

    latcolumn = grep("Latitude", names(data), ignore.case = T)
    loncolumn = grep("Longitude", names(data), ignore.case = T)
    if (length(latcolumn) > 0)
      LAT_DD = data[, latcolumn]
    if (length(loncolumn) > 0)
      LON_DD = data[, loncolumn]

    if (length(LAT_DD) == 0)
      LAT_DD = lat
    if (length(LON_DD) == 0)
      LON_DD = lon

    T_UID = rep(uid, length(TEMP))
    if (length(LAT_DD) == 1)
      LAT_DD = rep(LAT_DD, length(TEMP))
    if (length(LON_DD) == 1)
      LON_DD = rep(LON_DD, length(TEMP))

    LAT_DD = as.numeric(LAT_DD)
    LON_DD = as.numeric(LON_DD)

    ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, stringsAsFactors =  F)
  }

  if (grepl("CTS", fn)) {
    data = read.csv(fn)
    data = data[which(data$Serial_number == subset),]
    ret$Observed_Depth = unique(data$Log.Depth.m.)
    #Assuming CTS data always celsius
    ret$UnitsforTemp = "Celsius"

    ret$RecorderType = unique(data$Recorder)
    ret$RecorderID = unique(data$Serial_number)

    T_DATE = "NA"
    datecolumn = grep("Date", names(data), ignore.case = T)

    timecolumn = grep("Time", names(data), ignore.case = T)

    ds = data[, datecolumn][1]

    if (grepl("-", ds)) {
      T_DATE = lubridate::ymd_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = "America/Halifax")
    }
    if (grepl("/", ds)) {
      T_DATE = lubridate::dmy_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = "America/Halifax")
    }


    tempcolumn = grep("Temperature", names(data), ignore.case = T)
    if (length(tempcolumn) == 0)
      tempcolumn = grep("Temp", names(data), ignore.case = T)
    TEMP = as.numeric(data[, tempcolumn])

    LAT_DD = trimws(unlist(strsplit(header[grep("latitude", header, ignore.case = TRUE)], ": "))[2])
    LON_DD = trimws(unlist(strsplit(header[grep("longitude", header, ignore.case = TRUE)], ": "))[2])

    latcolumn = grep("Latitude", names(data), ignore.case = T)
    loncolumn = grep("Longitude", names(data), ignore.case = T)
    if (length(latcolumn) > 0)
      LAT_DD = data[, latcolumn]
    if (length(loncolumn) > 0)
      LON_DD = data[, loncolumn]

    if (length(LAT_DD) == 0)
      LAT_DD = lat
    if (length(LON_DD) == 0)
      LON_DD = lon

    T_UID = rep(uid, length(TEMP))
    if (length(LAT_DD) == 1)
      LAT_DD = rep(LAT_DD, length(TEMP))
    if (length(LON_DD) == 1)
      LON_DD = rep(LON_DD, length(TEMP))

    LAT_DD = as.numeric(LAT_DD)
    LON_DD = as.numeric(LON_DD)


    ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, stringsAsFactors =  F)
    td <-
      seconds_to_period(difftime(ret$data$T_DATE[2], ret$data$T_DATE[1], units = "secs"))
    ret$RecordingRate = sprintf('%02d:%02d:%02d', td@hour, minute(td), second(td))
    ret$Port = NULL

    ret$StartDate = T_DATE[1]
    ret$EndDate = T_DATE[length(T_DATE)]

    known = T
  }


  ##Need to complete simialar to how minilog was done above
  if (any(grepl("Sea-Bird", header))) {
    known = T
    type = "seabird"
  }
  header = gsub("Temp", "Temperature", header)
  if (any(grepl("Date", header[1])) &
      any(grepl("Temperature", header[1])) &
      known == F) {
    known = T

    ret$RecorderType = "Unknown"
    ret$RecorderID = "No_Header"

    type = "noheader"
    header = header[1]
    data = read.csv(fn)
    if (ncol(data) == 1)
      data =  read.delim(fn)
    ret$UnitsforTemp = "Unknown"
    ##If fahrenheit we should convert
    if (any(grepl("°C", header, ignore.case = TRUE)))
      ret$UnitsforTemp = "Celsius"
    if (any(grepl(" C", header, ignore.case = TRUE)))
      ret$UnitsforTemp = "Celsius"
    if (any(grepl("°F", header, ignore.case = TRUE)))
      ret$UnitsforTemp = "Fahrenheit"
    T_DATE = "NA"
    datecolumn = grep("date", names(data), ignore.case = T)
    # if (any(grepl("yyyy-mm-dd", header, ignore.case = TRUE))) {
    timecolumn = grep("time", names(data), ignore.case = T)
    #  if (any(grepl("hh:mm:ss", header, ignore.case = TRUE))) {
    ds = data[, datecolumn][1]

    if (grepl("-", ds)) {
      T_DATE = lubridate::ymd_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = "America/Halifax")
    }
    if (grepl("/", ds)) {
      T_DATE = lubridate::dmy_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = "America/Halifax")
    }



    tempcolumn = grep("Temperature", names(data), ignore.case = T)
    if (length(tempcolumn) == 0)
      tempcolumn = grep("Temp", names(data), ignore.case = T)
    TEMP = as.numeric(data[, tempcolumn])
    if (ret$UnitsforTemp == "Fahrenheit") {
      TEMP = ((TEMP - 32) * 5) / 9
      ret$UnitsforTemp = "Celsius"
    }
    LAT_DD = trimws(unlist(strsplit(header[grep("latitude", header, ignore.case = TRUE)], ": "))[2])
    LON_DD = trimws(unlist(strsplit(header[grep("longitude", header, ignore.case = TRUE)], ": "))[2])

    latcolumn = grep("Latitude", names(data), ignore.case = T)
    loncolumn = grep("Longitude", names(data), ignore.case = T)
    if (length(latcolumn) > 0)
      LAT_DD = data[, latcolumn]
    if (length(loncolumn) > 0)
      LON_DD = data[, loncolumn]

    if (length(LAT_DD) == 0)
      LAT_DD = lat
    if (length(LON_DD) == 0)
      LON_DD = lon

    T_UID = rep(uid, length(TEMP))
    if (length(LAT_DD) == 1)
      LAT_DD = rep(LAT_DD, length(TEMP))
    if (length(LON_DD) == 1)
      LON_DD = rep(LON_DD, length(TEMP))

    LAT_DD = as.numeric(LAT_DD)
    LON_DD = as.numeric(LON_DD)

    ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, stringsAsFactors =  F)
    td <-
      seconds_to_period(difftime(ret$data$T_DATE[2], ret$data$T_DATE[1], units = "secs"))
    ret$RecordingRate = sprintf('%02d:%02d:%02d', td@hour, minute(td), second(td))
    ret$Port = NULL
    ret$Observed_Depth = NULL
    ret$File = NULL


  }
  if (!known) {
    stop(paste("Could not determine file type of: ", fn, sep = ""))
  }

  ret$data = ret$data[which(!is.na(ret$data$T_DATE)),]

  ret$data$LON_DD = abs(ret$data$LON_DD) * -1
  return(ret)
}
#' @title acoustic_file_handler
#' @description  Internal function. Acoustic data is structured in opposition to project structure so this seperate handler was created to process and write data to the oracle tables
#' @param fn The data to process
#' @param fn The place to write the plot
#' @import lubridate ggplot2 scales
#' @return True or False
#' @export
acoustic_file_handler = function(data, fn) {

  #handle date formatting issues here first
  gf = guess_formats(data$date, c("mdy", "dmy", "ymd", "ydm"))
  gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
  if(gf == "%m-%d-%Y") data$date = mdy(data$date)
  if(gf == "%d-%m-%Y") data$date = dmy(data$date)
  if(gf == "%Y-%d-%m") data$date = ydm(data$date)
  if(gf == "%Y-%d-%m") data$date = ymd(data$date)

  gf = guess_formats(data$deploy_date, c("mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
  gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
  if(gf == "%m-%d-%Y %H:%M") data$deploy_date = mdy_hm(data$deploy_date)
  if(gf == "%m-%d-%Y %H:%M:%S") data$deploy_date = mdy_hms(data$deploy_date)
  if(gf == "%d-%m-%Y %H:%M:%S") data$deploy_date = dmy_hms(data$deploy_date)
  if(gf == "%d-%m-%Y %H:%M") data$deploy_date = dmy_hm(data$deploy_date)
  if(gf == "%Y-%m-%d %H:%M:%S") data$deploy_date = ymd_hms(data$deploy_date)
  if(gf == "%Y-%m-%d %H:%M") data$deploy_date = ymd_hm(data$deploy_date)

  #handle the case where not haul date provided
  if (!any(as.character(data$recover_date) != "") || all(is.na(data$recover_date))) {

  }
  else{
  gf = guess_formats(data$recover_date, c("mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
  gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
  if(gf == "%m-%d-%Y %H:%M") data$recover_date = mdy_hm(data$recover_date)
  if(gf == "%m-%d-%Y %H:%M:%S") data$recover_date = mdy_hms(data$recover_date)
  if(gf == "%d-%m-%Y %H:%M:%S") data$recover_date = dmy_hms(data$recover_date)
  if(gf == "%d-%m-%Y %H:%M") data$recover_date = dmy_hm(data$recover_date)
  if(gf == "%Y-%m-%d %H:%M:%S") data$recover_date = ymd_hms(data$recover_date)
  if(gf == "%Y-%m-%d %H:%M") data$recover_date = ymd_hm(data$recover_date)
  }


  mmpid = split(data, data$station_name)
  for (k in 1:length(mmpid)) {
    mm_sub = mmpid[[k]]
    print(as.character(mm_sub$station_name[1]))
    #In case where deployment date is same a record date
    if(length(unique(mm_sub$deploy_date)) > 50){ #Pick some number of unreasonable deployments
      mm_sub$deploy_date = min(mm_sub$deploy_date)
    }
    mdpid = split(mm_sub, as.character(mm_sub$deploy_date))
    #loop through each deployment
    for (j in 1:length(mdpid)) {
      md_sub = mdpid[[j]]

      #Remove min and max entries and duplicates
      ind = which(
        as.character(md_sub$description) == "Temperature" |
          as.character(md_sub$description) == "Average temperature" |
          as.character(md_sub$description) == "ambient_mean_deg_c"
      )
      md_sub = md_sub[ind, ]
      md_sub = md_sub[!duplicated(paste(as.character(md_sub$date), md_sub$receiver, sep = "-")), ]

      if (length(ind) == 0) {
        print("No 'Temperatrue', 'ambient_mean_deg_c' or 'Average temperature' feilds")
        next()
      }


      #Order by date
      # md_sub$sdate = dmy(as.character(md_sub$date), tz = "UTC")
      #
      # if (any(is.na(md_sub$sdate))) {
      #   md_sub$sdate = ymd(as.character(md_sub$date), tz = "UTC")
      # }
      # if (any(is.na(md_sub$sdate))) {
      #   md_sub$sdate = mdy(as.character(md_sub$date), tz = "UTC")
      # }
      md_sub = md_sub[order(md_sub$date), ]

      muid = paste(as.character(md_sub$station_name[1]),
                   as.character(length(which(
                     grepl(as.character(md_sub$station_name[1]), pkg.env$T_UID)
                   )) + 1),
                   sep = "-")
      loc = NA
      pro = NA
      if (grepl("CBS", as.character(md_sub$station_name[1]))) {
        loc = "Cabot Strait"
        pro = "OTN Cabot Strait"
      }
      if (grepl("HFX", as.character(md_sub$station_name[1]))) {
        loc = "South of Halifax"
        pro = "OTN Halifax"
      }
      if (grepl("V2LSCF", as.character(md_sub$catalognumber[1]))) {
        loc = "SABMPA"
        pro = "V2LSCF"
      }
      span = interval(md_sub$date[1], md_sub$date[2])

      rate = as.character(as.period(span, unit = "hours"))

      rate = unlist(strsplit(rate, " "))
      rate = gsub("[^0-9.-]", "", rate)
      rate = paste(rate[1], rate[2], rate[3], sep = ":")
      units = NA
      if (any(grepl("°F", md_sub$units))) {
        md_sub$data = ((as.numeric(as.character(
          md_sub$data
        )) - 32) * 5) / 9
        units = "Celsius"

      }
      if (any(grepl("°C", md_sub$units)))
        units = "Celsius"

      hauldate = md_sub$recover_date[1]
      #handle the case where not haul date provided
      if (!any(as.character(data$recover_date) != "") || all(is.na(data$recover_date))) {
        hauldate = max(md_sub$date)
      }

      ##..Add others here##

      # hauldate = dmy_hm(as.character(md_sub$recover_date[1]), tz = "UTC")
      # if (any(is.na(hauldate))) {
      #   hauldate = ymd_hms(as.character(md_sub$recover_date[1]), tz = "UTC")
      # }
      # setdate = dmy_hm(as.character(md_sub$deploy_date[1]), tz = "UTC")
      # if (any(is.na(setdate))) {
      #   setdate = ymd_hms(as.character(md_sub$deploy_date[1]), tz = "UTC")
      # }


      dx = data.frame(
        muid,
        as.character(md_sub$station_name[1]),
        pro,
        loc,
        unlist(strsplit(
          as.character(md_sub$receiver[1]), "-"
        ))[1],
        as.character(md_sub$rcv_serial_no[1]),
        md_sub$deploy_date[1],
        hauldate,
        rate,
        NA,
        fn,
        md_sub$dep_lat[1],
        md_sub$dep_long[1],
        NA,
        units,
        "Temp_Only One_Day_Buffer_Exclusion",
        NA,
        md_sub$deploy_date[1] + days(1),
        hauldate - days(1),
        stringsAsFactors = F
      )
      names(dx) = c(
        "M_UID",
        "PID",
        "PROJECT",
        "LOCATION",
        "RECORDER_TYPE",
        "RECORDER_ID",
        "START_DATE",
        "END_DATE",
        "RECORDING_RATE",
        "PORT",
        "FILE_S",
        "M_LAT_DD",
        "M_LON_DD",
        "OBSERVED_DEPTH_METERS",
        "UNITS_FOR_TEMP",
        "QAQC",
        "NOTES",
        "HAUL_DATE_START",
        "HAUL_DATE_END"
      )
      muiddx = rep(muid, nrow(md_sub))


      basedat = data.frame(
        muiddx,
        md_sub$date,
        md_sub$dep_lat,
        md_sub$dep_long,
        md_sub$data,
        stringsAsFactors =  F
      )
      names(basedat) = c("T_UID", "T_DATE", "LAT_DD", "LON_DD", "TEMP")

      if (dx$HAUL_DATE_END > dx$HAUL_DATE_START) {
        basedat = trim_to_dates(basedat, dx$HAUL_DATE_START, dx$HAUL_DATE_END, FALSE)
      }
      else{
        print("Not Enough DATA")
        next()
      }
      if (nrow(basedat) < 1) {
        print("Not Enough DATA")
        next()

      }

      if (!muid %in% pkg.env$M_UID) {
        drv = DBI::dbDriver("Oracle")
        con3 =  ROracle::dbConnect(
            drv,
            username = oracle.snowcrab.user,
            password = oracle.snowcrab.password,
            dbname = oracle.snowcrab.server
          )
        if(pkg.env$testwrite){

          ROracle::dbWriteTable(
            con3,
            name = "SC_TEMPERATURE_META_TEST",
            append = T,
            row.names = F,
            value = dx
          )
        }else{
        ROracle::dbWriteTable(
          con3,
          name = "SC_TEMPERATURE_META",
          append = T,
          row.names = F,
          value = dx
        )
        }
       # M_UID <<- c(M_UID, muid)
        assign('M_UID', c(pkg.env$M_UID, muid), pkg.env)
        ROracle::dbDisconnect(con3)
      }

      if (!muid %in% pkg.env$T_UID) {
        drv <- DBI::dbDriver("Oracle")
        con5 =
          ROracle::dbConnect(
            drv,
            username = oracle.snowcrab.user,
            password = oracle.snowcrab.password,
            dbname = oracle.snowcrab.server
          )

        if(pkg.env$testwrite){

          ROracle::dbWriteTable(
            con5,
            name = "SC_TEMPERATURE_BASE_TEST",
            append = T,
            row.names = F,
            value = basedat
          )
        }else{
        ROracle::dbWriteTable(
          con5,
          name = "SC_TEMPERATURE_BASE",
          append = T,
          row.names = F,
          value = basedat
        )
        }
        alluid = paste0(unique(basedat$T_UID))
        #T_UID <<- c(T_UID, alluid)
        assign('T_UID', c(pkg.env$T_UID, alluid), pkg.env)
        ROracle::dbDisconnect(con5)
      }
      basedat$T_DATE = as.POSIXct(basedat$T_DATE)
      p = ggplot(data = basedat, main = muid, aes(x = T_DATE, y = TEMP)) + geom_line() + ggtitle(muid) + xlab("Date") + ylab("Temp °C")
      p = p + scale_x_datetime(labels = date_format("%Y-%m"),
                               breaks = date_breaks("months")) + theme(axis.text.x = element_text(angle = 45))
      if (!dir.exists(file.path(dirname(fn), "ReceiverPlots")))
        dir.create(file.path(dirname(fn), "ReceiverPlots"))
      ggsave(
        p,
        width = 10,
        height = 10,
        filename = file.path(
          dirname(fn),
          "ReceiverPlots",
          paste(muid, ".pdf", sep = "")
        )
      )


    }
  }
}


#' @title trim_to_dates
#' @description  Internal function. Simply trim data to specified dates
#' @param data The data to subset
#' @param start the start time of desired data
#' @param end the end time of desired data
#' @param expand TRUE, want to go beyond start and end if values are the same
#' @import lubridate
#' @return dataframe of new trimed data
#' @export
trim_to_dates = function(data, start,	end, expand = T) {
  dat = NULL
  if (start == end) {
    if (expand)
      end = end + hours(24)
  }

  for (i in 1:length(start)) {
    dx = data[which(data$T_DATE >= start[i] & data$T_DATE <= end[i]),]
    dx$T_UID = gsub("-1", paste("-", as.character(i), sep = ""), dx$T_UID)
    dat = rbind(dat, dx)
  }


  return(dat)
}

#' @title auto_filter
#' @description  Internal function. Suggests better bottom times based on various inputs
#' @param da The data to inform
#' @param lat the latitude needed to get closest station temperatures
#' @param lon the longitude needed to get closest station temperatures
#' @param uid the unique id
#' @import lubridate
#' @return dataframe of best guess of down range from da
#' @export
auto_filter = function(da = NA,
                       lat = NA,
                       lon = NA,
                       uid = NA) {

  assign('metafile', system.file("extdata", "Station Inventory EN.csv", package = "SCTemperature"), pkg.env)

  #hw = read.csv(file.path(meta.dir, "weather_gc_station_inventory.csv"))
  hw = read.csv(pkg.env$metafile)
  hw = hw[which(!is.na(hw$HLY.First.Year)),]

  hw = hw[which(hw$HLY.First.Year < year(da$T_DATE[1])),]

  hw = hw[which(hw$HLY.Last.Year >= year(da$T_DATE[nrow(da)])),]

  p1 = as.matrix(cbind(
    hw$Longitude..Decimal.Degrees.,
    hw$Latitude..Decimal.Degrees.
  ))
  p2 = c(as.numeric(lon), as.numeric(lat))

  dist = distHaversine(p1, p2, r = 6378137)
  station = hw$Station.ID[which(dist == min(dist))]
  ext_DATE <<- da$T_DATE
  alldx = NULL
  dateframe = unique(date(da$T_DATE))
  yd = stringr::str_split(as.character(dateframe), "-", simplify = T)
  yd = paste(yd[, 1], yd[, 2], sep = "-")
  yd = unique(yd)

  for (i in 1:length(yd)) {
    dax = yd[i]
    url = paste(
      "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=",
      station,
      "&Year=",
      unlist(strsplit(dax, "-"))[1],
      "&Month=",
      unlist(strsplit(dax, "-"))[2],
      "&Day=12&timeframe=1&submit= Download+Data",
      sep = ""
    )

    download.file(url, "test.csv")
    header = readLines("test.csv", n = 50)
    ind = grep("Date/Time", header)
    dx = read.csv("test.csv", skip = ind - 1)
    alldx = rbind(alldx, dx)
  }

  alldx$Date = ymd_hm(as.character(alldx$Date.Time))
  alldx$Date = alldx$Date + minutes(1)
  alldx = alldx[which(alldx$Date >= da$T_DATE[1] &
                        alldx$Date <= da$T_DATE[nrow(da)]),]

  da$airTemp = NA
  for (i in 1:nrow(da)) {
    da$airTemp[i] = alldx$'Temp..Â.C.'[which(min(abs(da$T_DATE[i] - alldx$Date)) == abs(da$T_DATE[i] - alldx$Date))]
  }

  da$difatwat = da$TEMP - da$airTemp
  da$difatwat[which(da$difatwat < 5)] = 0
  da$difatwat[which(is.na(da$difatwat))] = 0
  da$var = NA
  for (i in 1:nrow(da)) {
    if (i == 1)
      da$var[i] =  0
    else
      da$var[i] = abs(da$TEMP[i] - da$TEMP[i - 1])
  }
  da$dailyflut = NA
  udate = unique(date(da$T_DATE))
  for (i in 1:length(udate)) {
    ind = which(date(da$T_DATE) == udate[i])
    da$dailyflut[ind] =  rep(max(da$TEMP[which(date(da$T_DATE) == udate[i])]) - min(da$TEMP[which(date(da$T_DATE) == udate[i])]), length(ind))
  }
  da$dailyflutair = NA
  udate = unique(date(da$T_DATE))
  for (i in 1:length(udate)) {
    ind = which(date(da$T_DATE) == udate[i])
    da$dailyflutair[ind] =  rep(max(da$airTemp[which(date(da$T_DATE) == udate[i])]) - min(da$airTemp[which(date(da$T_DATE) == udate[i])]), length(ind))
  }
  da$pretidal = NA
  da$posttidal = NA


  #for(i in 1:(length(udate)-5)){
  for (i in 1:nrow(da)) {
    enddate = da$T_DATE[i] + days(2)
    startdate = da$T_DATE[i] - days(2)
    eind = which(da$T_DATE >= enddate)[1]
    sind = which(da$T_DATE <= startdate)[length(which(da$T_DATE <= startdate))]
    if (length(eind) == 0)
      eind = i
    if (length(sind) == 0)
      sind = i
    if (is.na(eind))
      eind = i
    if (is.na(sind))
      sind = i

    preind = sind:i
    postind = i:eind
    while (length(preind) < 50)
      preind = c(preind, preind[length(preind)] + 1)
    while (length(postind) < 50)
      postind = c(postind[1] - 1, postind)


    if (length(unique(da$TEMP[preind])) == 1)
      prex = 1
    else
      prex = findfrequency(da$TEMP[preind])

    if (length(unique(da$TEMP[postind])) == 1)
      postx = 1
    else
      postx = findfrequency(da$TEMP[postind])


    if (prex != 1)
      if (prex != i)
        da$pretidal[i] = as.numeric(difftime(da$T_DATE[i], da$T_DATE[i - prex], units = "hours"))
    if (postx != 1)
      if (postx != i)
        da$posttidal[i] = as.numeric(difftime(da$T_DATE[i + postx], da$T_DATE[i], units = "hours"))

  }


  da$prehrvar = NA
  da$posthrvar = NA
  da$confidence = 0

  #for(i in 1:(length(udate)-5)){
  for (i in 1:nrow(da)) {
    enddate = da$T_DATE[i] + hours(2)
    startdate = da$T_DATE[i] - hours(2)
    eind = which(da$T_DATE >= enddate)[1]
    sind = which(da$T_DATE <= startdate)[length(which(da$T_DATE <= startdate))]
    if (length(eind) == 0)
      eind = i
    if (length(sind) == 0)
      sind = i
    if (is.na(eind))
      eind = i
    if (is.na(sind))
      sind = i

    preind = sind:i
    postind = i:eind
    while (length(preind) < 4)
      preind = c(preind, preind[length(preind)] + 1)
    while (length(postind) < 4)
      postind = c(postind[1] - 1, postind)


    prex = var(da$TEMP[preind])
    postx = var(da$TEMP[postind])

    #Also check for major fluxations



    if (abs(da$TEMP[sind] - da$TEMP[eind]) > 5)
      da$confidence[sind:eind] = 100
    ###################################

    #  plot(da$T_DATE[postind], da$TEMP[postind], col = "blue", type = "l")
    #plot(da$T_DATE[preind], da$TEMP[preind], col = "blue", type = "l")

    #
    da$prehrvar[i] = prex
    da$posthrvar[i] = postx

  }

  da$avar = da$posthrvar + da$prehrvar / 2

  da$atmos = F
  da$atmos[which(da$posttidal >= 22 & da$posttidal <= 26)] = T
  da$atmos[which(da$pretidal >= 22 & da$pretidal <= 26)] = T

  da$tid = F
  da$tid[which(da$posttidal >= 8 & da$posttidal <= 16)] = T
  da$tid[which(da$pretidal >= 8 & da$pretidal <= 16)] = T

  da$diftemp = abs(da$TEMP - da$airTemp)

  da$confidence[which(da$TEMP > 18)] = da$confidence[which(da$TEMP > 18)] + 1
  da$confidence[which(da$TEMP > 20)] = da$confidence[which(da$TEMP > 20)] + 2
  da$confidence[which(da$TEMP > 22)] = da$confidence[which(da$TEMP > 22)] + 5
  da$confidence[which(da$TEMP > 24)] = da$confidence[which(da$TEMP > 24)] + 100
  da$confidence[which(da$TEMP > 26)] = da$confidence[which(da$TEMP > 26)] + 100

  da$confidence = da$confidence - (da$avar * 10)
  da$atmosv = 0
  da$atmosv[which(da$atmos == T)] = 10
  da$tidv = 0
  da$tidv[which(da$tid == T)] = 12
  da$confidence = da$confidence + da$atmosv
  da$confidence = da$confidence + da$difatwat


  oneday = da$T_DATE[1] + days(1)
  oneind = which(da$T_DATE >= oneday)[1]
  da$confidence[1:oneind] = 100

  oneday = da$T_DATE[length(da$T_DATE)] - days(1)
  oneind = which(da$T_DATE >= oneday)[1]
  da$confidence[oneind:length(da$T_DATE)] = 100

  lo <- smooth.spline(da$T_DATE, da$confidence, spar = .85)


  #png(file = file.path(meta.dir,"plots", paste(uid, ".png", sep = "")))
  #plot(da$T_DATE, da$TEMP, col = "blue", type = "l")
  #lines(da$T_DATE, da$airTemp, col = "red")

  ind = which(lo$y < 5)

  #lines(da$T_DATE[ind], da$TEMP[ind], col = "red")
  #lines(da$T_DATE, da$confidence, col = "green")
  #lines(da$T_DATE, lo$y, col = "black")
  #lines(da$T_DATE, da$dailyflut, col = "purple")
  #lines(da$T_DATE, da$dailyflutair, col = "yellow")
  #lines(da$T_DATE, da$atmosv, col = "brown")
  da$lo = lo$y
  #lines(da$T_DATE, da$atmosv, col = "green", lwd = 4)

  #lines(da$T_DATE, da$posthrvar*10, col = "green", lwd = 4)
  #lines(da$T_DATE, da$tidv, col = "orange", lwd = 4)

  #dev.off()
  return(da[ind,])

}
