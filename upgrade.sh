#!/bin/bash

root=$(pwd)
json=$(jq -c '.' './submodules.json')

for module in $(echo $json | jq -cr '.modules[]'); do
  moduleName=$(echo $module | jq -cr ".module")
  dependencies=$(echo $module | jq -cr ".dependencies[]")
  moduleGoPath=$(echo $json | jq -cr ".list.\"$moduleName\".goPath")
  moduleRootPath=$(echo $json | jq -cr ".list.\"$moduleName\".rootPath")
  moduleUrl=$(echo $json | jq -cr ".list.\"$moduleName\".url")
  moduleBranch=$(echo $json | jq -cr ".list.\"$moduleName\".branch")

  cd $root/$moduleGoPath
  depListStr=$(echo $module | jq -r '.dependencies | join(", ")')

  for dep in $dependencies; do
    depUrl=$(echo $json | jq -cr ".list.\"$dep\".url")
    depBranch=$(echo $json | jq -cr ".list.\"$dep\".branch")

    go get $depUrl@$depBranch
  done

  cd $root/$moduleRootPath

  git add .
  git commit -m "'$depListStr' dependencies upgrade"
  # git push HEAD:$branch
  git push
  git checkout $branch
  git pull

  go mod tidy
done
