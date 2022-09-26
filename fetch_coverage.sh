#!/bin/bash

echo "Enter GitLab API Token:"
read -s token

curl --location --header "PRIVATE-TOKEN: $token" "https://git.cs.uni-kl.de/api/v4/projects/1975/jobs/artifacts/main/download?job=test" --output coverage.zip
unzip coverage.zip
rm coverage.zip