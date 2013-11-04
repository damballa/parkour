#! /bin/sh -e

#git checkout master
lein doc
git checkout gh-pages
sleep 1
find tmp/codox -type f -exec touch {} +
rsync -r tmp/codox/ .
#git add .
#git commit
#git push -u damballa gh-pages
#git checkout master
