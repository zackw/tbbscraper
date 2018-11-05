#! /bin/sh

cd /home/mahmoods/java-projects/topic-models/CensorshipLDA
exec java -Xms2g -Xmx20g -XX:-UsePerfData -cp bin:/home/mahmoods/java-packages/mallet-2.0.7/lib/mallet-deps.jar:/home/mahmoods/java-packages/json-simple-1.1.1.jar cylab/mallet/runfiles/IATopicChangeDetection
