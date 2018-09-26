
create_users:
	python create_data.py --host ${MHOST} --drop --users

create_products:
	python3 create_data.py --host ${MHOST} --drop --products

