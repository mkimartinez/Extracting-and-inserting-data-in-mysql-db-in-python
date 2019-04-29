import pymysql
import pandas as pd
filename="C:\\Users\\marti\\Desktop\\Data_analysis_thesis\\Data\\Newyork_data_final 2019-04-29 20_09_49.csv"
f=open(filename,'r')
fString =f.read()
# print(fString)
# convert to string

flist=[]
for line in fString.split('\n'):
	flist.append(line.split(','))
# print(flist[0][22])
column_names =['UID', 'PID', 'CatID', 'CategoryName', 
'Lat', 'Lon', 'Time', 'Date', 'DayOfWeek', 
'Season', 'ExactTime', 'Dayslot', 'ContextID', 
'Combined_context', 'HourSlot', 'Total_User_checkins', 
'Total_PID_Checkins', 'new_date', 'MinTemp', 
'MaxTemp', 'AverageTemp', 'TempKey', 'WeatherDesc', 
'Humidity', 'Pressure', 'Visibilty', 'Precipittaion', 
'CloudCover', 'WindSpeed', 'MoonPhase', 
'HeatIndex', 'moon_illumination', 'WeatherSummary', 
'Combined_weather']
print(len(column_names))
print(column_names[33])
db = pymysql.connect("localhost","root","","test")
cursor=db.cursor()
cursor.execute("DROP TABLE IF EXISTS CHECKIN_DATA")
ID=1; UID=column_names[0];	PID=column_names[1];	CatID=column_names[2]
CategoryName=column_names[3];Lat=column_names[4];Lon=column_names[5]
Time=column_names[6];Date=column_names[7];	DayOfWeek=column_names[8]
Season=column_names[9];	ExactTime=column_names[10];	Dayslot=column_names[11];ContextID=column_names[12]
Combined_context=column_names[13];	HourSlot=column_names[14]	
Total_User_checkins=column_names[15];Total_PID_Checkins=column_names[16]
new_date=column_names[17];	MinTemp=column_names[18]
MaxTemp=column_names[19];AverageTemp =column_names[20]; TempKey=column_names[21];
WeatherDesc =column_names[22]; Humidity=column_names[23]; Pressure=column_names[24];Visibilty=column_names[25]
Precipittaion=column_names[26]; CloudCover=column_names[27];WindSpeed=column_names[28];MoonPhase=column_names[29] 
HeatIndex=column_names[30] ; moon_illumination =column_names[31];  WeatherSummary=column_names[32] 
Combined_weather=column_names[33]

queryCreateTable=f"""CREATE TABLE CHECKIN_DATA(
					{UID} varchar(255) not null,
					{PID} varchar(255) not null,
					{CatID} varchar(255) not null,
					{CategoryName} varchar(255) not null,
					{Lat} varchar(255) not null,
					{Lon} varchar(255) not null,
					{Time} varchar(255) not null,
					{Date} varchar(255) not null,
					{DayOfWeek} varchar(255) not null,
					{Season} varchar(255) not null,
					{ExactTime} varchar(255) not null,
					{Dayslot} varchar(255) not null,
					{ContextID} varchar(255) not null,
					{Combined_context} varchar(255) not null,
					{HourSlot} varchar(255) not null,
					{Total_User_checkins} varchar(255) not null,
					{Total_PID_Checkins} varchar(255) not null,
					{new_date} varchar(255) not null,
					{MinTemp} varchar(255) not null,
					{MaxTemp} varchar(255) not null,
					{AverageTemp} varchar(255) not null,
					{TempKey} varchar(255) not null,
					{WeatherDesc} varchar(255) not null,
					{Humidity} varchar(255) not null,
					{Pressure} varchar(255) not null,
					{Visibilty} varchar(255) not null,
					{Precipittaion} varchar(255) not null,
					{CloudCover} varchar(255) not null,
					{WindSpeed} varchar(255) not null,
					{MoonPhase} varchar(255) not null,
					{HeatIndex} varchar(255) not null,
					{moon_illumination} varchar(255) not null,
					{WeatherSummary} varchar(255) not null,
					{Combined_weather} varchar(255) not null
					)
				"""
cursor.execute(queryCreateTable)
del flist[0]
# print([flist[0]])
rows=''
for i in range(len(flist)-1):
	rows +=f"""('{flist[i][1]}','{flist[i][2]}','{flist[i][3]}','{flist[i][4]}','{flist[i][5]}',
	'{flist[i][6]}','{flist[i][7]}','{flist[i][8]}','{flist[i][8]}','{flist[i][8]}','{flist[i][11]}',
	'{flist[i][11]}','{flist[i][12]}','{flist[i][13]}','{flist[i][14]}',
				'{flist[i][15]}','{flist[i][16]}','{flist[i][17]}','{flist[i][18]}','{flist[i][19]}','{flist[i][20]}',
				'{flist[i][21]}','{flist[i][22]}','{flist[i][23]}','{flist[i][24]}','{flist[i][25]}','{flist[i][26]}',
				'{flist[i][27]}','{flist[i][28]}','{flist[i][29]}',
				'{flist[i][30]}','{flist[i][31]}','{flist[i][32]}','{flist[i][33]}'
	)"""
	if i !=len(flist)-2:
		rows +=','
query_insert ="INSERT INTO CHECKIN_DATA VALUES"+rows
print(flist[i][33])
print(rows)
try:
	cursor.execute(query_insert)
	db.commit()
except:
	print("Error occureed")
	db.rollback()
db.close()