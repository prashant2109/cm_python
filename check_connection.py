import mysql.connector as mysql

db = mysql.connect(
    host = "172.16.20.52",
    user = "root",
    passwd = "tas123"
)

cursor = db.cursor()

## executing the statement using 'execute()' method
cursor.execute("SHOW DATABASES")

## 'fetchall()' method fetches all the rows from the last executed statement
databases = cursor.fetchall() ## it returns a list of all databases present
print databases
