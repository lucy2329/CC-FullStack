sudo docker stop $(sudo docker ps -a -q)
sudo docker rm $(sudo docker ps -a -q)
truncate -s 0 filewrites.txt
cd workers
sudo docker build  -t worker:latest .
cd ..
sudo docker-compose up
