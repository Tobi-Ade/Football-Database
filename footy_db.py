"""
Importing necessary libraries
"""
import pandas as pd 

import sqlite3  

"""
Creating a database class with different methods,
each of which perform a specific  function
"""
class DB():
	"""
	creating constructor object for the DB class
	"""
	def __init__(self):
		self.connect = None

	def create_connection(self, db_name): 
		"""
		This function creates a connection to a new or pre-existing database
		params: database name 
		returns: connection to database
		rtype: object
		"""
		try:
			self.connect = sqlite3.connect(db_name)
			print("Connection successfully created")
		except sqlite3.Error:
			print("Unable to create connection")
			self.connect = sqlite3.connect(db_name)
		return self.connect


	def create_table(self, table_data):
		"""
		This function creates a database table
		params: table schema
		returns: creats database table
		rtype: object
		"""
		cursor = self.connect.cursor()
		try:
			print("creating table...")
			cursor.execute(table_data)
			self.connect.commit()
			print("table created")
		except:
			print('Table already exists')
		self.connect.commit()

	def add_items(self, items_list):
		"""
		This function adds multiple items to a table in the database
		params: list of items to be added
		returns: populated database
		rtype: object
		"""
		cursor = self.connect.cursor()
		cursor.executemany("INSERT INTO ballers VALUES (?,?,?,?,?)", items_list)
		self.connect.commit()
	
	def show_table(self):
		"""
		This function displays table and its data
		params: None
		return: database table 
		rtype: object
		"""
		cursor = self.connect.cursor()
		data = cursor.execute("SELECT * FROM ballers").fetchall()
		return data

	def delete_row_by_id(self, id):
		"""
		This function deletes a row from a table using the row id 
		params: database connection, id of row to be deleted
		returns: None
		"""
		cursor = self.connect.cursor()
		cursor.execute("DELETE FROM  WHERE rowid = (?)", id)
		self.connect.commit()
	
	def delete_table(self):
		"""
		This function deletes the entire table from the database
		params: None
		returns: None
		"""
		cursor = self.connect.cursor()
		try:
			print('Deleting Table...')
			cursor.execute("DROP TABLE ballers")
			print("Table Deleted")
		except sqlite3.Error:
			print("Unable to delete table")

		self.connect.commit()


	def create_csv_data(self, data, file_name):
		"""
		This fucntion saves the databse data to a specifed file
		"""
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
Outlining the table schema
"""
table_data = ("""CREATE TABLE IF NOT EXISTS ballers  (
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
Creating a main function to call all the class methods 
"""
def main():
	db = DB() #creates an instance of the class 
	db.create_connection('Footy_db')
	db.create_table(table_data)
	db.add_items(ballers_list)
	print(db.show_table())
	db.create_csv_data(ballers_list, 'players_data')

"""
Invoking the main function
"""
if __name__ == "__main__":
	main()
