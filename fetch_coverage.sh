#!/bin/bash

branch=${1:-"main"}

echo -n "Enter GitLab API Token: "
read -s token
echo

curl --location --header "PRIVATE-TOKEN: $token" "https://git.cs.uni-kl.de/api/v4/projects/1975/jobs/artifacts/$branch/download?job=test" --output coverage.zip
unzip coverage.zip
rm coverage.zip