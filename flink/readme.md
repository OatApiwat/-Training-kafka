flink run -py /home/pyflink/flink-sql.py
docker cp flink-job.py <jobmanager-container-id>:/home/pyflink/
docker exec -it <jobmanager-container-id> /bin/bash
https://pypi.org/project/apache-flink/#files
https://pypi.org/project/apache-flink-libraries/#files