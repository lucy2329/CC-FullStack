# CC-FullStack

Assignment 1:
-upload files to instance
-setup flask, nginx, guinicorn
-run the app as : gunicorn app:app -w 4 -b localhost:5000

Assignment 2:
-Upload the folders to instance
-change ip in the app.py files to your respective instance ip address
-go to each folder and run: sudo docker-compose up

Assignment 3:
-Set up a load balancer with rules on AWS 
-Create 2 instances, upload rides to 1 and users to the other.
-Change the ip's as required.
-run sudo docker-compose up in each of the instance  after cd'ing into the folder.
-send requests over load balancer ip

Final Project:
-Create 3 instances.
-create load balancer.
-Place each folder in a different instance.
-Change the ip's as required.
-cd into the folder in each instance and run the script "run-all.sh".
- you might have to run sudo docker system prune --volumes -a incase instance runs out of memory.
- send requests over load balancer.

Testing: 
- Test1.postman_collection.json is a postman collection we made on our own set of requests 
