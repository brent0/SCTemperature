#' @title SCT_hobo
#' @description Function that standardizes HOBO formatted files
#' @param fn The data to process
#' @param ret List of already procesed SCHEADER data
#' @param uid Pass determined uid
#' @param lat Pass determined lat
#' @param lon Pass determined lon
#' @param depth Pass determined depth
#' @import lubridate
#' @return List of formatted data
#' @export
SCT_hobo = function(fn, ret, uid, lat, lon, depth){

  ind = grep("Water Detect,Host Connect,Button Down", readLines(fn, n = 100), ignore.case = TRUE)
  header = readLines(fn, n = ind)
  if (any(grepl("<SCHEADER>", header)) &&
      any(grepl("</SCHEADER>", header))) {
    header = header[-(grep("<SCHEADER>", header):grep("</SCHEADER>", header))]
  }
  header = gsub("\t", "", header)
  header = gsub("\\*C", "°C", header)
  header = gsub("\\*F", "°F", header)
  header = gsub(",,", "", header)

  data = read.csv(fn, skip = ind-1, header = T)

  #Hobo files dont have much header info, Filling in with best guess in case incomplete SCHEADER entries
  ret$Project = "RandomHobo"
  ret$RecorderType = "Hobob"
  ret$RecorderID = trimws(unlist(strsplit(header[grep("Serial Number", header, ignore.case = TRUE)], ":"))[2])

  dtc = data[,1]
  dtfor = names(data[1])
  zone = substr(dtfor, unlist(gregexpr("..", dtfor, fixed = T))[1], unlist(gregexpr("..", dtfor, fixed = T))[2])
  zone = gsub("\\.", "", zone)
  offset = substr(dtfor, unlist(gregexpr("..", dtfor, fixed = T))[2], nchar(dtfor))
  offset = gsub("\\.", "", offset)
  offset = gsub("0", "", offset)
  tzstr = ""
  if(grepl("-", offset)){
    tzstr = paste(zone, offset, sep="")
  }else{
    tzstr = paste(zone, offset, sep="+")
  }
  tzstr = OlsonNames()[grep(tzstr, OlsonNames(), fixed = T)]
  dtc = ymd_hms(dtc)
  dtc = force_tz(dtc, tzstr)
  dtc = with_tz(dtc, "America/Halifax")
  ret$StartDate = dtc[1]
  ret$EndDate = dtc[length(dtc)]
  ret$File = fn

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


  ret$StartDate = dmy_hms(ret$StartDate, tz = tz)

  ret$EndDate = dmy_hms(ret$EndDate, tz = tz)

  #NORMALISE DATA
  #Convert date
  T_DATE = dtc

  td = seconds_to_period(difftime(T_DATE[6], T_DATE[5], units = "secs"))
  ret$RecordingRate = sprintf('%02d:%02d:%02d', td@hour, minute(td), second(td)) #ret$File = basename(fn)

  tempcolumn = grep("Temp", names(data), ignore.case = T)

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
  #No depth for HOBO
  DEPTH_M = rep(NA, length(TEMP))

  LAT_DD = as.numeric(LAT_DD)
  LON_DD = as.numeric(LON_DD)

  INWATER = rep(0, length(TEMP))
  in.ind = which(data$Water.Detect == "In")
  out.ind = which(data$Water.Detect == "Out")
  for(k in 1:length(in.ind)){
    INWATER[in.ind[k]:out.ind[k]] = 1
  }
  ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, DEPTH_M, INWATER, stringsAsFactors =  F)

  rmind = which(is.na(ret$data$TEMP))
  if(length(rmind)>0){
    ret$data = ret$data[-which(is.na(ret$data$TEMP)),]
  }

  return(ret)
}

#' @title SCT_hobo2
#' @description Function that standardizes HOBO formatted files
#' @param fn The data to process
#' @param ret List of already procesed SCHEADER data
#' @param uid Pass determined uid
#' @param lat Pass determined lat
#' @param lon Pass determined lon
#' @param depth Pass determined depth
#' @import lubridate
#' @return List of formatted data
#' @export
SCT_hobo2 = function(fn, ret, uid, lat, lon, depth){

  ind = grep(make.names("#,Date-Time (ADT),Ch: 1 - Temperature   (°C )"), make.names(readLines(fn, n = 100)), ignore.case = TRUE)
  header = readLines(fn, n = ind)
  if (any(grepl("<SCHEADER>", header)) &&
      any(grepl("</SCHEADER>", header))) {
    header = header[-(grep("<SCHEADER>", header):grep("</SCHEADER>", header))]
  }
  header = gsub("\t", "", header)
  header = gsub("\\*C", "°C", header)
  header = gsub("\\*F", "°F", header)
  header = gsub(",,", "", header)

  data = read.csv(fn, skip = ind-1, header = T)

  #Hobo files dont have much header info, Filling in with best guess in case incomplete SCHEADER entries
  ret$Project = "RandomHobo"
  ret$RecorderType = "Hobob"
  ret$RecorderID = trimws(unlist(strsplit(header[grep("Serial Number", header, ignore.case = TRUE)], ","))[2])

  dtc = data[,2]
  dtfor = names(data[2])
  zone = substr(dtfor, unlist(gregexpr("..", dtfor, fixed = T))[1], unlist(gregexpr(".", dtfor, fixed = T))[4])
  zone = gsub("\\.", "", zone)
 # offset = substr(dtfor, unlist(gregexpr("..", dtfor, fixed = T))[2], nchar(dtfor))
  #offset = gsub("\\.", "", offset)
 # offset = gsub("0", "", offset)
  # tzstr = ""
  # if(grepl("-", offset)){
  #   tzstr = paste(zone, offset, sep="")
  # }else{
  #   tzstr = paste(zone, offset, sep="+")
  # }
  if(zone == "ADT"){
    tzstr = "Canada/Atlantic"
  }
  dtc = mdy_hms(dtc)
  dtc = force_tz(dtc, tzstr)

  ret$StartDate = dtc[1]
  ret$EndDate = dtc[length(dtc)]
  ret$File = fn

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


 # ret$StartDate = dmy_hms(ret$StartDate, tz = tz)

 # ret$EndDate = dmy_hms(ret$EndDate, tz = tz)

  #NORMALISE DATA
  #Convert date
  T_DATE = dtc

  td = seconds_to_period(difftime(T_DATE[6], T_DATE[5], units = "secs"))
  ret$RecordingRate = sprintf('%02d:%02d:%02d', td@hour, minute(td), second(td)) #ret$File = basename(fn)

  tempcolumn = grep("Temp", names(data), ignore.case = T)

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
  #No depth for HOBO
  DEPTH_M = rep(NA, length(TEMP))

  LAT_DD = as.numeric(LAT_DD)
  LON_DD = as.numeric(LON_DD)

  # INWATER = rep(0, length(TEMP))
  # in.ind = which(data$Water.Detect == "In")
  # out.ind = which(data$Water.Detect == "Out")
  # for(k in 1:length(in.ind)){
  #   INWATER[in.ind[k]:out.ind[k]] = 1
  # }
  ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, DEPTH_M, stringsAsFactors =  F)

  rmind = which(is.na(ret$data$TEMP))
  if(length(rmind)>0){
    ret$data = ret$data[-which(is.na(ret$data$TEMP)),]
  }

  return(ret)
}
