
import pandas as pd 

import sqlite3  

"""
Outlining the table structure
"""
table_data = ("""CREATE TABLE ballers (
    first_name text,
    last_name text, 
    position text,
    club text,
    country text
)""")

"""
List of items to be inserted into the table
"""
ballers_list = [
	('Neymar', 'Jr', 'FW', 'PSG', 'Brazil'),
	('Kevin', 'De Bruyne', 'AM', 'Man City', 'Belgium'),
	('Joao', 'Felix', 'SS', 'Aletico', 'Portugal'),
	('Toni', 'Kroos', 'CM', 'Madrid', 'Germany'),
	('Gabriel', 'Jesus', 'FW', 'Arsenal', 'Brazil'),
	('Bernado', 'Silva', 'AM', 'Man City', 'Portugal'),
	('Phil', 'Jones', 'CB', 'Man Utd', 'England'),
	('Ferran', 'Torres', 'FW', 'Barcelona', 'Spanish'),
	('Gianluigi', 'Buffon', 'GK', 'Parma', 'Italy'),
	('Lionel', 'Messi', 'FW', 'PSG', 'Argentina')
]

"""
This function creates a connection to a new or pre-existing database
params: database_name 
returns: connection to database
rtype: object
"""
def create_connection(db_name): 
	conn = None
	try:
		conn = sqlite3.connect(db_name)
		print("COnnection successfully created")
	except sqlite3.Error:
		print("Unable to create connection")
	return conn
"""
This function creates a database table
params: table_schema, connection
"""
def create_table(connection, table_data):
	c = connection.cursor()
	print("creating table...")
	c.execute(table_data)
	connection.commit()
	print("table created")


"""
This function displays table data
params: database connection
return: table data
"""
def show_table(connection):
	c = connection.cursor()
	data = c.execute("SELECT * FROM ballers").fetchall()
	return data


"""
This function creates a csv file using specified table data
params: table data, name of file to be created
return: csv file
rtype: csv
"""
def create_csv_data(data, file_name):
	writer = []
	for item in data:
		dict_writer = {
			"first_name" : item[0],
			'last_name' : item[1],
			'position' : item[2],
			"club" : item[3],
			"country" : item[4]

		}
		writer.append(dict_writer)
	Baller_df = pd.DataFrame(writer, columns=["first_name", "last_name", "position", "club" , "country" ])
	print("Converting to csv in progress")
	Baller_df.to_csv(file_name, index=False)
	print("Conversion to csv is successful")

"""
This function deletes a row from a table using the specified id number
params: database connection, id of row to be deleted
"""
def delete_row_by_id(connection, id):
	c = connection.cursor()
	c.execute("DELETE FROM TABLE WHERE rowid = (?)", id)
	connection.commit()

"""
This function adds multiple items to
params: database connection, id of row to be deleted
"""
def add_items(connection, items_list):
	c = connection.cursor()
	c.executemany("INSERT INTO ballers VALUES (?,?,?,?,?)", items_list)
	connection.commit()

"""
This function deletes the entire table from the database
params: database connection
"""
def delete_table(connection):
	c = connection.cursor()
	print("Deleting table...")
	c.execute("DROP TABLE ballers")
	print("Table deleted...")
	connection.commit()

"""
Testing the functions 
"""
if __name__ == "__main__":
	connection = create_connection("Footy_db")

	create_table(connection, table_data)

	add_items(connection, ballers_list)
	

	data = show_table(connection)

	for items in data:
		print(items)
	
	create_csv_data(ballers_list, "Ballers_data")

	
	











