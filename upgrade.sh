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

  echo "cd $root/$moduleGoPath"
  cd $root/$moduleGoPath
  depListStr=$(echo $module | jq -r '.dependencies | join(", ")')

  for dep in $dependencies; do
    depUrl=$(echo $json | jq -cr ".list.\"$dep\".url")
    depBranch=$(echo $json | jq -cr ".list.\"$dep\".branch")

    echo "go get $depUrl@$depBranch"
    go get $depUrl@$depBranch
  done

  echo "cd $root/$moduleRootPath"
  cd $root/$moduleRootPath

  echo "go mod tidy"
  go mod tidy

  echo "git add ."
  git add .
  echo "git commit -m \"'$depListStr' dependencies upgrade\""
  git commit -m "'$depListStr' dependencies upgrade"
  echo "git push"
  git push
  echo "git checkout $branch"
  git checkout $branch
  echo "git pull"
  git pull
done
