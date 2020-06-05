cur.execute("DROP TABLE IF EXISTS transactions2;");
cur.execute("DROP TABLE IF EXISTS albums;");
cur.execute("DROP TABLE IF EXISTS employees;");
cur.execute("DROP TABLE IF EXISTS sales;");

DROP TABLE IF EXISTS transactions2;
DROP TABLE IF EXISTS albums;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS sales;


CREATE TABLE IF NOT EXISTS transactions2 (
	transaction_id int,
	customer_name varchar,
	cashier_id int,
	year int
);

CREATE TABLE IF NOT EXISTS albums (
	album_id int,
	transaction_id int,
	album_name int
);

CREATE TABLE IF NOT EXISTS employees (
	employee_id int,
	employee_name varchar
);

CREATE TABLE IF NOT EXISTS sales (
	transaction_id int,
	amount_spent int
);

SELECT transactions2.transaction_id, customer_name, employee_name as cashier_name, year, album_name as album_sold, amount_spent as amount_sold 
from
(
	(
		(
			transactions2 
			JOIN sales 
			on transactions2.transaction_id = sales.transaction_id
		)
		JOIN albums 
		on transactions2.transaction_id = albums.transaction_id
	)
	JOIN employees
	on transactions2.cashier_id = employees.employee_id
)


CREATE TABLE IF NOT EXISTS transactions (
	transaction_id int,
	customer_name varchar,
	cashier_id int,
	year int,
	amount_spent int
);

INSERT INTO transactions (transaction_id,customer_name,cashier_id,year, amount_spent)

1, "Amanda", 1, 2000, 40
2, "Toby", 1, 2000, 19
3, "Max", 2, 2018, 50

CREATE TABLE IF NOT EXISTS cashier_sales (
	transaction_id int,
	cashier_name varchar,
	cashier_id int,
	amount_spent int
)

(transaction_id,cashier_name,cashier_id,amount_spent)

1, "Sam", 1, 40
1, "Sam", 1, 40
3, "Bob", 2, 45


select cashier_name, sum(amount_spent) from cashier_sales group by cashier_name;





try: 
    cur.execute('''CREATE TABLE IF NOT EXISTS customer_transactions (
		cashier_id int,
		store_id int,
		spend decimal
	)''')
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
#Insert into all tables 
try: 
    cur.execute('''INSERT INTO customer_transactions ( cashier_id, store_id, spend)
	VALUES (%s,%s,%s)''',(1,1,20.50))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
try: 
    cur.execute('''INSERT INTO customer_transactions ( cashier_id, store_id, spend)
	VALUES (%s,%s,%s)''',(2,1,35.21))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)



try: 
	cur.execute("DROP TABLE IF EXISTS customer")
	cur.execute("DROP TABLE IF EXISTS item_purchased")
	cur.execute("DROP TABLE IF EXISTS store")
    cur.execute('''CREATE TABLE IF NOT EXISTS customer(
				customer_id int,
				name varchar,
				rewards bool
				)''')
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute('''INSERT INTO customer(
					customer_id,
					name,
					rewards,
				) VALUES (%s,%s,%s)''',(
					1,
					"Amanda",
					True,
				))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute('''INSERT INTO customer(
					customer_id,
					name,
					rewards,
				) VALUES (%s,%s,%s)''',(
					2,
					"Toby",
					False,
				))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute('''CREATE TABLE IF NOT EXISTS item_purchased(
				customer_id int,
				item_number int,
				item_name varchar
				)''')
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute('''INSERT INTO item_purchased(
					customer_id,
					item_number,
					item_name
				) VALUES (%s,%s,%s)''',(
					1,
					1,
					"Rubber Soul"
				))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
try: 
    cur.execute('''INSERT INTO item_purchased(
					customer_id,
					item_number,
					item_name
				) VALUES (%s,%s,%s)''',(
					2,
					3,
					"Let It Be"
				))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute('''CREATE TABLE IF NOT EXISTS store(
					store_id int,
					state varchar
				)''')
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute('''INSERT INTO store(
					store_id int,
					state varchar
				) VALUES (%s,%s)''',(
					1,
					"CA"
				))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute('''INSERT INTO store(
				) VALUES (%s,%s)''',(
					2,
					"WA"
				))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)


SELECT customer.name, store.store_id, store.state, item_purchased.item_name, customer.rewards FROM (
	(
		(
			customer_transactions JOIN customer ON customer_transactions.customer_id=customer_id
		)JOIN store ON customer_transactions.store_id = store.store_id 
	)JOIN item_purchased ON customer_transactions.customer_id =  item_purchased.customer_id
) WHERE customer_transactions.spend > 30


SELECT sum(spend) FROM customer_transactions WHERE customer_id==2



SELECT artist, song, song_length FROM sessions WHERE session_id = 338 and item_in_session=4
SELECT artist, song FROM users WHERE user_id =10 and session_id= 182 ORDER BY item_in_session
SELECT first_name, last_name FROM songs WHERE song = 'All Hands Against His Own'
