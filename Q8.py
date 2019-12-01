import pandas as pd
import sqlite3

con = sqlite3.connect("data/email.sqlite")

cur = con.cursor()

df = pd.read_sql_query("select * from emails",con)

con.close()