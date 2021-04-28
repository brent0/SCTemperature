#' @title SCT_cts
#' @description Function that standardizes cts formatted files
#' @param fn The data to process
#' @param ret List of already procesed SCHEADER data
#' @param subset Subset the file by serial number. Cts Data is one big file
#' @param uid Pass determined uid
#' @param lat Pass determined lat
#' @param lon Pass determined lon
#' @param depth Pass determined depth
#' @import lubridate
#' @return List of formatted data
#' @export
SCT_cts = function(fn, ret, subset, uid, lat, lon, depth){
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
    datedat = paste(data[, datecolumn], data[, timecolumn], sep = " ")
    gf = guess_formats(datedat, c("mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
    gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
    if(gf == "%m-%d-%Y %H:%M") T_DATE =  lubridate::mdy_hm(datedat)
    if(gf == "%m-%d-%Y %H:%M:%S") T_DATE =  lubridate::mdy_hms(datedat)
    if(gf == "%d-%m-%Y %H:%M:%S") T_DATE =  lubridate::dmy_hms(datedat)
    if(gf == "%d-%m-%Y %H:%M") T_DATE =  lubridate::dmy_hm(datedat)
    if(gf == "%Y-%m-%d %H:%M:%S") T_DATE =  lubridate::ymd_hms(datedat)
    if(gf == "%Y-%m-%d %H:%M") T_DATE =  lubridate::ymd_hm(datedat)

    # T_DATE = lubridate::ymd_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = "America/Halifax")
  }
  if (grepl("/", ds)) {
    datedat = paste(data[, datecolumn], data[, timecolumn], sep = " ")
    gf = guess_formats(datedat, c("mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
    gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
    if(gf == "%m/%d/%Y %H:%M") T_DATE =  lubridate::mdy_hm(datedat)
    if(gf == "%m/%d/%Y %H:%M:%S") T_DATE =  lubridate::mdy_hms(datedat)
    if(gf == "%d/%m/%Y %H:%M:%S") T_DATE =  lubridate::dmy_hms(datedat)
    if(gf == "%d/%m/%Y %H:%M") T_DATE =  lubridate::dmy_hm(datedat)
    if(gf == "%Y/%m/%d %H:%M:%S") T_DATE =  lubridate::ymd_hms(datedat)
    if(gf == "%Y/%m/%d %H:%M") T_DATE =  lubridate::ymd_hm(datedat)

    # T_DATE = lubridate::dmy_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = "America/Halifax")
  }


  tempcolumn = grep("Temperature", names(data), ignore.case = T)
  if (length(tempcolumn) == 0)
    tempcolumn = grep("Temp", names(data), ignore.case = T)
  TEMP = as.numeric(data[, tempcolumn])

  depthcolumn = grep("Depth.m", names(data), ignore.case = T)
  DEPtH_M = as.numeric(data[, depthcolumn])

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


  ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, DEPTH_M, stringsAsFactors =  F)
  td = seconds_to_period(difftime(ret$data$T_DATE[2], ret$data$T_DATE[1], units = "secs"))
  ret$RecordingRate = sprintf('%02d:%02d:%02d', td@hour, minute(td), second(td))
  ret$Port = NULL

  ret$StartDate = T_DATE[1]
  ret$EndDate = T_DATE[length(T_DATE)]

    return(ret)
}
