import pymysql
import pandas as pd
filename="C:\\Users\\xxxx\\Desktop\\ThesisData\\Newyork_data_final .csv"
f=open(filename,'r')
data = f.read()
print(data[0])
flist=[]
for line in data.split('\n'):
	flist.append(line.split(','))
print(flist[0][3])
db = pymysql.connect("localhost","root","","test")
cursor=db.cursor()
cursor.execute("DROP TABLE IF EXISTS CHECKIN_DATA")
UID=flist[0][1];	PID=flist[0][2];	CatID=flist[0][3]
CategoryName=flist[0][4];Lat=flist[0][5];Lon=flist[0][6]
Time=flist[0][7];Date=flist[0][8];	DayOfWeek=flist[0][9]
Season=flist[0][10];	ExactTime=flist[0][11];	Dayslot=flist[0][12]
Combined_context=flist[0][14];	HourSlot=flist[0][15]	
Total_User_checkins=flist[0][16];Total_PID_Checkins=flist[0][18]
new_date=flist[0][19];	MinTemp=flist[0][20];MaxTemp=flist[0][21];WeatherDesc=flist[0][22];
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
					{Combined_context} varchar(255) not null,
					{HourSlot} varchar(255) not null,
					{Total_User_checkins} varchar(255) not null,
					{Total_PID_Checkins} varchar(255) not null,
					{new_date} varchar(255) not null,
					{MinTemp} varchar(255) not null,
					{MaxTemp} varchar(255) not null,
					{WeatherDesc} varchar(255) not null
					)
				"""
cursor.execute(queryCreateTable)
del flist[0]
rows=''
for i in range(len(flist)-1):
	rows+=f"""({flist[i][0]},{flist[i][1]},{flist[i][2]},
	{flist[i][3]},{flist[i][4]},{flist[i][5]},
	{flist[i][6]},{flist[i][8]},{flist[i][9]},
	{flist[i][10]},{flist[i][11]},{flist[i][12]},
	{flist[i][13]},{flist[i][15]},,{flist[i][16]},{flist[i][18]},
	{flist[i][19]},{flist[i][20]},{flist[i][21]},{flist[i][22]})"""
	if i !=len(flist)-2:
		rows+= ','
print(rows)
query_insert ="INSERT INTO CHECKIN_DATA VALUES"+rows
# ('U1023','Loc27258','Cat_54','Office','40.639',	'-74.217',	'Weekend', 'Dec 16 11:29:29 +0000 2012','Dec 16 2012','Weekend','Winter','11:29:29','Midday','Coxt33','Winter_Weekend_Midday','11','/16/2012','6','8','Overcast')"

# print(rows)
try:
	cursor.execute(query_insert)
	db.commit()
except:
	print("Error occureed")
	db.rollback()
db.close()