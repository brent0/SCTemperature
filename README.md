# SCTemperature
Write bottom temperatures from various sensors to Database

Contains methods for standardizing data and filtering data to when it is believed to be at bottom. 
    Run regenStationInventory() periodically to update station list which is used to get air temperatures at nearest weather station at a specific time.
    Populate(fn) is the perfered function to begin data entry. It adds all files contained in the fn directory. To use this method, files must have custom header lines added. Here is a template to use:
   <SCHEADER>
               <USERID>SCT</USERID>  *Call it what you like, if none provided will use 'UNK'
               <PROJECT>SAB-MPA Acoustic Tagging</PROJECT>  *Enter a project description, will try to populate from native header if none provided
               <LOCATION>St.Anns Bank</LOCATION>  *Enter a study location, will try to populate from native header if none provided
               <PORT>Louisbourg</PORT>  *Enter port of operations, will try to populate from native header if none provided
    - For sensors that log positions such as mobile sensors, latitudes and longitudes will be entered from the respective columns and these tags are not needed            
    - If latitude and longitude can be referenced from the native header info then these tags are not needed
    - OTHERWISE these are mandatory, enter where sensor is thought to be at bottom
               <LAT> 46.000181,45.98245,45.98356,46.13332,46.09002,46.17857,46.18343 </LAT> 
               <LON> -59.15358,-59.18483,-59.31505,-59.16851,-59.088,-58.9815,-58.97552 </LON>
    - Enter start and end dates where sensor is thought to be at bottom
    - If none provided, code will attempt to narrow the window. 
    - If none provided and it turns out to have more than one deployment, ONLY the first position and depth will be used.
               <STARTDATE>22/04/2018T22:07:00,23/04/2018T08:08:00,23/04/2018T16:37:00,23/04/2018T05:05:00,24/04/2018T10:00:00,24/04/2018T14:00:00,24/04/2018T16:00:00   </STARTDATE>
               <ENDDATE>23/04/2018T06:43:00,23/04/2018T13:36:00,23/04/2018T20:04:00,24/04/2018T07:05:00,24/04/2018T12:00:00,24/04/2018T16:00:00,24/04/2018T23:53:00   </ENDDATE>
    - Enter depths where sensor is thought to be at bottom 
               <DEPTH>44, 25, 48, NA, 52, 75, NA</DEPTH>    
               <DEPTHUNITS> fathoms </DEPTHUNITS> *Enter the units for the depth tag. If none will assume meters. (meters, fathoms or feet). Will be converted to meters.
               <TZ>America/Halifax</TZ> *Enter user OS recognized time zone, if none provided will be set to "America/Halifax"  
               <NOTES> This is a test!! </NOTES> *Enter any notes you like
    </SCHEADER>
     Populate_by_worksheet(fn) was the initial method of entry. May still work however it may not be as efficient. Reads a csv of Metadata and searches the file column to get the raw data. Here is an example structure:  
     with the following column names:
     Location,	LFA,	Project,	RecorderType,	RecorderID,	ProjectID,	StartDate,	EndDate,	RecordingRate,	Port,	File..,	LAT.Decimal.Degrees,	LON.Decimal.Degrees,	Observed_Depth,	UnitsforTemp,	QAQC,	Notes,	HaulDate_Start,	HaulDate_End
     example entry:
     Woods Harbour,	34,	Lobster Recruitment	Minilog-II-T,	351015,	AAA634,	26/10/2015,	16/06/2016,	1:00:00,	1332,	4,	43.15,	-65.75,	NA,	Celsius,	NA,	NA,	08/05/2016,	14/05/2016
