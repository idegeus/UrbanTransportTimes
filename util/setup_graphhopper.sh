# Manually starts a Graphhopper instance.

# This file coordinates the current requirement for grasshopper, and then 
# Command for manually starting a Graphhopper Docker: (-d removed for testing purposes, --rm for removing automatically added)
docker run -p 8989:8989 -v /Users/ivo/Desktop/thesis/DUTTv2/1-data:/1-data --env JAVA_OPTS="-Xmx8g -Xms8g" --entrypoint /bin/bash israelhikingmap/graphhopper -c "cd ../ && java -Xmx8g -Xms8g -jar /graphhopper/*.jar server /1-data/2-gh/config-duttv2.src.yml"
# docker run -d -p 8989:8989 --rm -v /Users/ivo/Desktop/DUTTv2/1-data:/1-data --env JAVA_OPTS="-Xmx8g -Xms8g" --entrypoint /bin/bash israelhikingmap/graphhopper -c "cd ../ && java -Xmx8g -Xms8g -jar /graphhopper/*.jar server /1-data/2-gh/config-duttv2.src.yml"


rm -rf ./1-data/2-gh/graph-cache