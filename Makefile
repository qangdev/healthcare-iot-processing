clean:
	sudo rm -rf docker
	sudo docker-compose down -v

permission:
	sudo chown qang:qang -R docker
	sudo chmod 777 -R docker

rundb:
	sudo docker-compose up