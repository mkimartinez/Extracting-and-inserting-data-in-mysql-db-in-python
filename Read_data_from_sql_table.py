import pandas as pd 
import pymysql

db=pymysql.connect("localhost","root","","cheka")
cursor =db.cursor()

query= "SELECT * FROM tp_menu WHERE is_dev=2"
list1=[]
try:
	cursor.execute(query)
	data= cursor.fetchall()
	print(list(data)[0])
	list1=list(data)
except:
	db.rollback()
# for row in data.split("\n"):
# 	list.append(row.split(","))
df= pd.DataFrame(list1)
print(df)
drop_query ="DELETE FROM tp_users where id=3"
cursor.execute(drop_query)
db.commit()
query_insert ="INSERT INTO tp_menu VALUES((48,'Programming', 'gear', 0, 100, 'New_Menu/index', 0, '', '', 2))"
try:
	cursor.execute(query_insert)
	db.commit()
except:
	db.rollback()
	print("Error ")
print(df.shape)
db.close()

