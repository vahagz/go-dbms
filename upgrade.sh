#!/bin/bash

GREEN='\033[92m'
RESET='\033[0m'

root=$(pwd)
json=$(jq -c '.' './submodules.json')

export GOPROXY=direct

for module in $(echo $json | jq -cr '.modules[]'); do
  moduleName=$(echo $module | jq -cr ".module")
  dependencies=$(echo $module | jq -cr ".dependencies[]")
  moduleGoPath=$(echo $json | jq -cr ".list.\"$moduleName\".goPath")
  moduleRootPath=$(echo $json | jq -cr ".list.\"$moduleName\".rootPath")
  moduleUrl=$(echo $json | jq -cr ".list.\"$moduleName\".url")
  moduleBranch=$(echo $json | jq -cr ".list.\"$moduleName\".branch")

  echo -e "${GREEN}cd $root/$moduleGoPath${RESET}"
  cd $root/$moduleGoPath
  depListStr=$(echo $module | jq -r '.dependencies | join(", ")')

  for dep in $dependencies; do
    depUrl=$(echo $json | jq -cr ".list.\"$dep\".url")
    depBranch=$(echo $json | jq -cr ".list.\"$dep\".branch")

    echo -e "${GREEN}go -u get $depUrl@$depBranch${RESET}"
    go get -u $depUrl@$depBranch
  done

  echo -e "${GREEN}cd $root/$moduleRootPath${RESET}"
  cd $root/$moduleRootPath

  echo -e "${GREEN}go mod tidy${RESET}"
  go mod tidy

  echo -e "${GREEN}git add .${RESET}"
  git add .
  echo -e "${GREEN}git commit -m \"'$depListStr' dependencies upgrade\"${RESET}"
  git commit -m "'$depListStr' dependencies upgrade"
  echo -e "${GREEN}git push${RESET}"
  git push
  echo -e "${GREEN}git checkout $branch${RESET}"
  git checkout $branch
  echo -e "${GREEN}git pull${RESET}"
  git pull
done
