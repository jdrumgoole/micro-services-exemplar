#MHOST=mongodb://localhost:27017/?replicaset=ecom
MHOST=mongodb+srv://jdrumgoole:erasmus@vendor-gl0aq.azure.mongodb.net/test?retryWrites=true
all:
	python create_data.py --host ${MHOST} --drop --users --products --baskets

create_users:
	python create_data.py --host ${MHOST} --drop --users

create_products:
	python3 create_data.py --host ${MHOST} --drop --products

create_baskets:
	python3 create_data.py --host ${MHOST} --drop --baskets

basket_generator:
	python3 basket_generator.py --host ${MHOST}

snap_db:
	python3 snap_db.py --host ${MHOST} --watch ECOM.baskets --snap ECOM.baskets_snap

init_server:
	@echo "Setting up replica set";\
	if [ -d "data" ];then\
		echo "Replica set Already configured in 'data' start with make start_server";\
	else\
		echo "Making new mlaunch environment in 'data'";\
		mlaunch init --replicaset --name "ecom";\
        fi
clean: clean_db
	rm -rf data

clean_db:
		python create_data.py --host ${MHOST} --drop

start_server:
	@echo "Starting MongoDB replica set"
	@if [ -d "data" ];then\
		mlaunch start;\
	else\
		echo "No mlaunch data, run make init_server";\
	fi

stop_server:
	@echo "Stopping MongoDB replica set"
	@if [ -d "data" ];then\
		mlaunch stop;\
	else\
		echo "No mlaunch data, run make init_server";\
	fi

