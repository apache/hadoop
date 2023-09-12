# How to build

1. Find a machine with an x86_64 architecture. This does not work on an ARM-based MacBook. A small EC2 instance is good for this.
2. Set the version in the pom files to the desired new version: `mvn versions:set -DnewVersion=3.2.2-deco2`. Note: Double-check with a grep if all versions have been updated. We have experienced missing updates in the past.
3. Run `./start-build-env.sh`. Once this is done, you can run `mvn install` to build locally.
4. Deployment to AWS CodeArtifact is also possible, but the required changes to the pom.xml file are not yet merged to this branch. `mvn deploy -Denforcer.skip -DskipTests`