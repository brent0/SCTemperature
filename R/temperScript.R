

pkg.env = new.env(parent = emptyenv())
assign('con', NULL, pkg.env)
assign('M_UID', NULL, pkg.env)
assign('T_UID', NULL, pkg.env)
assign('GlobLat', NULL, pkg.env)
assign('GlobLon', NULL, pkg.env)
assign('GlobDep', NULL, pkg.env)
assign('tz', "America/Halifax", pkg.env)
assign('testwrite', TRUE, pkg.env)



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
  assign('con', NULL, pkg.env)
  assign('M_UID', NULL, pkg.env)
  assign('T_UID', NULL, pkg.env)
  assign('GlobLat', NULL, pkg.env)
  assign('GlobLon', NULL, pkg.env)
  assign('GlobDep', NULL, pkg.env)
  assign('tz', "America/Halifax", pkg.env)
  assign('testwrite', TRUE, pkg.env)
  if(test){
    Sys.setenv(testwrite = T)
    assign('testwrite', TRUE, pkg.env)
  }
  else{
    Sys.setenv(testwrite = F)
    assign('testwrite', FALSE, pkg.env)
  }
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
    warning("Writing to temporary tables only!!!  Review SC_TEMPERATURE_META_TEST and SC_TEMPERATURE_BASE_TEST then call populate again with test = F ")
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
        manualmeta = read.csv(fl[i], stringsAsFactors = F)
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
        #Add other known non-header files here.
        # if(match some known file characteristics){}
        else{
          print("Unknown format")
        }

      }
      ## This enters if we are dealing with a file that has custom header data
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
#' @param ad Optional indicies that have already been added
#' @param uid The unique identifier for plotting
#' @import lubridate tcltk
#' @return data at new range
#' @export
click.temp = function(da = NA,
                      af = NA,
                      ad = NULL,
                      uid = NA) {



  #Set up plot range
  if(!is.null(af)){

  if ((nrow(af) / nrow(da)) < .05) {
    sdate = af$T_DATE[1] - days(5)
    edate = af$T_DATE[length(af$T_DATE)] + days(5)
    if (sdate > da$T_DATE[1])
      da = da[which(da$T_DATE >= sdate),]
    if (edate < da$T_DATE[length(da$T_DATE)])
      da = da[which(da$T_DATE <= edate),]
  }
  }

  yrange <- range(c(da$TEMP, da$TEMP + 4, da$TEMP - 4))

  xrange <- range(c(da$T_DATE[1], da$T_DATE[length(da$T_DATE)]))
  xy <- xy.coords(da$T_DATE, da$TEMP)
  xval <- xy$x
  yval <- xy$y

  result = NULL

  active = T

  while (active) {
      x11(width = 25, height = 15)
    par(mar = c(5, 8, 4, 4) + 0.1)
    plot(
      da$T_DATE,
      da$TEMP,
      axes = FALSE,
      ylim = yrange,
      xlim = xrange,
      xlab = 'Date',
      ylab = 'Temperature',
      type = "l",
      main = uid,
      lwd = 1
    )

    axis.POSIXct(1, at = seq(da$T_DATE[1], da$T_DATE[length(da$T_DATE)], by = "month"))
    axis(2)
    #Plot the filter
    if(!is.null(af)){
    lines(
      af$T_DATE,
      af$TEMP,
      axes = F,
      type = "p",
      col = "red",
      lwd = 1
    )
    }
    #Plot the filter
    if(!is.null(ad)){

    lines(
      da$T_DATE[ad],
      da$TEMP[ad],
      axes = F,
      type = "p",
      col = "green",
      lwd = 1
    )
    }
    if(!is.null(af)){
    points(af$T_DATE[1], af$TEMP[1], pch = 3, col = "red")
    points(af$T_DATE[length(af$T_DATE)], af$TEMP[length(af$TEMP)], pch = 3, col = "red")
}

    if(!is.null(da$INWATER)){

      par(new = TRUE)
      col = rep("red",nrow(da))
      col[which(da$INWATER == 1)] = "green"
      plot(da$T_DATE, da$INWATER, type = "p", xaxt = "n", col = col, yaxt = "n",
           ylab = "", xlab = "", ylim = c(0,1))
      par(new = TRUE)
         legend("topright", legend=c("In Water", "Out Water"),
             col=c("green", "red"), pch = c(1, 1), cex=0.8)
      }

    xy <- xy.coords(da$T_DATE, da$TEMP)
    x <- xy$x
    y <- xy$y
    Sys.sleep(1)
    zoom <- tkmessageBox(
      title = "ManualTD",
      message = "Would you like to zoom in on a time range by clicking start and end points?",
      icon = "info",
      type = "yesno"
    )


    if (grepl("yes", zoom)) {
      #Code that allow identifying clicks
      id = identify(x, y, n = 1, pos = TRUE)
      start = da$T_DATE[id$ind]
      points(xval[id$ind], yval[id$ind], col = "darkgreen", pch = 19)

      id2 = identify(x, y, n = 1, pos = TRUE)
      end = da$T_DATE[id2$ind]
      points(xval[id2$ind], yval[id2$ind], col = "darkgreen", pch = 19)
      xrange = range(c(da$T_DATE[id$ind], da$T_DATE[id2$ind]))
      active = T
      dev.off()
    }
    if (grepl("no", zoom)) {

    #Code that allow identifying clicks
    id = identify(x, y, n = 1, pos = TRUE)
    start = da$T_DATE[id$ind]
    points(xval[id$ind], yval[id$ind], col = "darkgreen", pch = 19)

    id2 = identify(x, y, n = 1, pos = TRUE)
    end = da$T_DATE[id2$ind]
    points(xval[id2$ind], yval[id2$ind], col = "darkgreen", pch = 19)

    res <- tkmessageBox(
      title = "ManualTD",
      message = "Are you happy with these values?",
      icon = "info",
      type = "yesno"
    )

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
         active = F
         dev.off()
    }
    if (grepl("no", res)) {
      xrange <- range(c(da$T_DATE[1], da$T_DATE[length(da$T_DATE)]))
      active = T
      dev.off()
    }
  }
}
  again = F
  res2 <- tkmessageBox(
     title = "ManualTD",
     message = "Are there more bottom profiles in this plot that need to be added?",
     icon = "info",
     type = "yesno")

  if (grepl("yes", res2)) {
     again = T
  }

  if (grepl("no", res2)) {
    again = F
  }
  return(list(res = result, again = again ))
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
                            depth = NULL,
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

  #Check if this is a minilog logger
  if (any(grepl("Minilog", header))) {
    known = T
    ret = SCT_minilog(fn, ret, uid, lat, lon, depth)
  }#End minilog handler

  #Check if this is cts data
  if (grepl("CTS", fn)) {
    known = T
    ret = SCT_cts(fn, ret, subset, uid, lat, lon, depth)
  } #End cts handler

  #Check if this is a seabird logger
  if (any(grepl("Sea-Bird", header))) {
    known = T
    ret = SCT_seabird(fn, ret, uid, lat, lon, depth)
  } #End Seabird handler

  #Check if this is a hobo logger
  if (any(grepl("Water Detect,Host Connect,Button Down", header))) {
    known = T
    ret = SCT_hobo(fn, ret, uid, lat, lon, depth)
  }#End hobo handler

  #Check to see if we can load even with no known types
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
    td = seconds_to_period(difftime(ret$data$T_DATE[2], ret$data$T_DATE[1], units = "secs"))
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
#' @import lubridate geosphere forecast
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
if(nrow(hw) >0){
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

  meantemp = ave(da$airTemp, da$TEMP)
  tx = da$TEMP - meantemp

#  da$difatwat[which(da$difatwat < 5)] = 0
#  da$difatwat[which(is.na(da$difatwat))] = 0
  da$var = NA
  for (i in 1:nrow(da)) {
    if (i == 1)
      da$var[i] =  0
    else
      da$var[i] = abs(da$TEMP[i] - da$TEMP[i - 1])
  }
#  da$dailyflut = NA
#  udate = unique(date(da$T_DATE))
#  for (i in 1:length(udate)) {
 #   ind = which(date(da$T_DATE) == udate[i])
#    da$dailyflut[ind] =  rep(max(da$TEMP[which(date(da$T_DATE) == udate[i])]) - min(da$TEMP[which(date(da$T_DATE) == udate[i])]), length(ind))
 # }
#  da$dailyflutair = NA
#  udate = unique(date(da$T_DATE))
 # for (i in 1:length(udate)) {
 #   ind = which(date(da$T_DATE) == udate[i])
 #   da$dailyflutair[ind] =  rep(max(da$airTemp[which(date(da$T_DATE) == udate[i])]) - min(da$airTemp[which(date(da$T_DATE) == udate[i])]), length(ind))
#  }
#  da$pretidal = NA
#  da$posttidal = NA


  #for(i in 1:(length(udate)-5)){
#  for (i in 1:nrow(da)) {
#    enddate = da$T_DATE[i] + days(2)
#    startdate = da$T_DATE[i] - days(2)
#    eind = which(da$T_DATE >= enddate)[1]
#    sind = which(da$T_DATE <= startdate)[length(which(da$T_DATE <= startdate))]
#
#     if (length(eind) == 0 )
#       eind = i
#     if (length(sind) == 0 )
#       sind = i
#     if (is.na(eind))
#       eind = i
#     if (is.na(sind))
#       sind = i
#
#     preind = sind:i
#     postind = i:eind
#     while (length(preind) < 50)
#       preind = c(preind, preind[length(preind)] + 1)
#     while (length(postind) < 50)
#       postind = c(postind[1] - 1, postind)
#     postind = postind[which(postind > 0)]
#
#     if (length(unique(da$TEMP[preind])) < 5)
#       prex = 1
#     else
#       prex = findfrequency(da$TEMP[preind])
#
#
#
#     if (length(unique(da$TEMP[postind])) < 5)
#       postx = 1
#     else
#       postx = findfrequency(da$TEMP[postind])
#     if (prex != 1)
#       if (prex != i)
#         da$pretidal[i] = as.numeric(difftime(da$T_DATE[i], da$T_DATE[i - prex], units = "hours"))
#     if (postx != 1)
#       if (postx != i)
#         da$posttidal[i] = as.numeric(difftime(da$T_DATE[i + postx], da$T_DATE[i], units = "hours"))
#   }


 # da$prehrvar = NA
 # da$posthrvar = NA
 # da$confidence = 0

  #for(i in 1:(length(udate)-5)){
  # for (i in 1:nrow(da)) {
  #   enddate = da$T_DATE[i] + hours(2)
  #   startdate = da$T_DATE[i] - hours(2)
  #   eind = which(da$T_DATE >= enddate)[1]
  #   sind = which(da$T_DATE <= startdate)[length(which(da$T_DATE <= startdate))]
  #   if (length(eind) == 0)
  #     eind = i
  #   if (length(sind) == 0)
  #     sind = i
  #   if (is.na(eind))
  #     eind = i
  #   if (is.na(sind))
  #     sind = i
  #
  #   preind = sind:i
  #   postind = i:eind
  #   while (length(preind) < 4)
  #     preind = c(preind, preind[length(preind)] + 1)
  #   while (length(postind) < 4)
  #     postind = c(postind[1] - 1, postind)
  #
  #
  #   prex = var(da$TEMP[preind])
  #   postx = var(da$TEMP[postind])
  #
  #   #Also check for major fluxations
  #
  #
  #
  #   if (abs(da$TEMP[sind] - da$TEMP[eind]) > 5)
  #     da$confidence[sind:eind] = 100
  #   ###################################
  #
  #   #  plot(da$T_DATE[postind], da$TEMP[postind], col = "blue", type = "l")
  #   #plot(da$T_DATE[preind], da$TEMP[preind], col = "blue", type = "l")
  #
  #   #
  #   da$prehrvar[i] = prex
  #   da$posthrvar[i] = postx
  #
  # }

#  da$avar = da$posthrvar + da$prehrvar / 2

 # da$atmos = F
 # da$atmos[which(da$posttidal >= 22 & da$posttidal <= 26)] = T
 # da$atmos[which(da$pretidal >= 22 & da$pretidal <= 26)] = T

 # da$tid = F
 # da$tid[which(da$posttidal >= 8 & da$posttidal <= 16)] = T
 # da$tid[which(da$pretidal >= 8 & da$pretidal <= 16)] = T
 da$confidence = .1
  #da$diftemp = abs(da$airTemp - da$TEMP)

  da$confidence[which(da$TEMP > 18)] = da$confidence[which(da$TEMP > 18)] + 1
  da$confidence[which(da$TEMP > 20)] = da$confidence[which(da$TEMP > 20)] + 2
  da$confidence[which(da$TEMP > 22)] = da$confidence[which(da$TEMP > 22)] + 3
  da$confidence[which(da$TEMP > 24)] = da$confidence[which(da$TEMP > 24)] + 3
  da$confidence[which(da$TEMP > 26)] = da$confidence[which(da$TEMP > 26)] + 3

  da$confidence[which(da$var > .25)] = da$confidence[which(da$var > .5)] + 5
  da$confidence[which(tx > 0)] = da$confidence[which(tx>0)] + 5

  #da$confidence = da$confidence - (da$avar * 10)
  #da$atmosv = 0
  #da$atmosv[which(da$atmos == T)] = 10
  #da$tidv = 0
 # da$tidv[which(da$tid == T)] = 12
  #da$confidence = da$confidence + da$atmosv
  #da$confidence = da$confidence + da$difatwat

  # if((da$T_DATE[length(da$T_DATE)] - da$T_DATE[1]) > days(3)){
  #   oneday = da$T_DATE[1] + days(1)
  #   oneind = which(da$T_DATE >= oneday)[1]
  #   if(is.logical(oneind) && !is.na(oneind)){
  #     da$confidence[1:oneind] = 100
  #   }
  #   else{
  #     da$confidence[1:length(da$confidence)] = 100
  #   }
  #   oneday = da$T_DATE[length(da$T_DATE)] - days(1)
  #   oneind = which(da$T_DATE >= oneday)[1]
  #   da$confidence[oneind:length(da$T_DATE)] = 100
  # }
  lo <- smooth.spline(da$T_DATE, da$confidence, spar = .25)


  #png(file = file.path(meta.dir,"plots", paste(uid, ".png", sep = "")))
  #plot(da$T_DATE, da$TEMP, col = "blue", type = "l")
  #lines(da$T_DATE, da$airTemp, col = "red")

  ind = which(lo$y < 1)

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
}else{
    return(NULL)
  }
}





#' @title decibar2depth
#' @description  Internal function. This function calculates depth in meters from pressure in decibars using Saunders and Fofonoff's method
#' @param P pressure, in decibars
#' @param lat degrees latitude
#' @param Del geopotential anomaly (defaults to 0)
#' @return depths converted from pressure
#' DEEP-SEA RES., 1976, 23, 109-111.
#' / UNITS:
#' //   PRESSURE     P      DECIBARS
#' //   LATITUDE     LAT    DEGREES
#' //   DEPTH        DEPTH  METERS
#' //   DTN. HEIGHT  DEL    DYN. METERS (geopotenial anomaly in J/kg:: assume 0)
#' // CHECKVALUE:
#' //   1.) DEPTH = 9712.653 M for P=10000 DECIBARS, LAT=30 DEG, DEL=0
#' //   ABOVE FOR STANDARD OCEAN: T=0 DEG CELSIUS; S=35 (PSS-78)
#' // ----------------------------------------------------------
#' // Original fortran code is found in:
#' //   UNESCO technical papers in marine science 44 (1983) -
#' //   'Algorithms for computation of fundamental properties of seawater'
#'  \url{http://www.code10.info/index.php?option=com_content&view=article&id=67:calculating-the-depth-from-pressure&catid=54:cat_coding_algorithms_seawater&Itemid=79}, \url{http://www.code10.info/index.php?option=com_content&view=article&id=67:calculating-the-depth-from-pressure&catid=54:cat_coding_algorithms_seawater&Itemid=79}
decibar2depth = function( P, lat, Del=0 ) {

    X = (sin( lat * pi / 180 ))^2 # convert degree decimal to RADs

    # GR=GRAVITY VARIATION WITH LATITUDE: ANON (1970) BULLETIN GEODESIQUE
    GR = 9.780318 * (1.0 + (5.2788E-3 + 2.36E-5 * X) * X) + 1.092E-6 * P
    DepthTerm = (((-1.82E-15 * P + 2.279E-10) * P - 2.2512E-5) * P + 9.72659) * P ## assuming (35 psu, 0 C, and P=pressure in decibars )
    DEPTH = DepthTerm / GR + Del / 9.8
    return (DEPTH)
}


